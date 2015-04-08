#!/usr/bin/env python

import ConfigParser
import sys
import os
import json
import socket
import base64
import struct
import traceback
from serf_master import SerfHandler, SerfHandlerProxy
from functools import reduce


cfg_sect_node = "node"
cfg_item_rpcaddr = "rpc_addr"

serf_msg_hostname = "hostname"
serf_msg_serf = "serf"
serf_msg_bindaddr = "bindaddr"
serf_msg_rpcaddr = "rpcaddr"
serf_msg_starthash = "starthash"
serf_msg_vnode = "vnode"

msg_cmd_size = 1
msg_body_size = 2
msg_extra_size = 1
msg_header_size = msg_cmd_size + msg_body_size + msg_extra_size
msg_itemid_size = 1
msg_itemlen_size = 2

msg_cmd_nodeinfo = 1
msg_cmd_vnodeinfo = 2

msg_item_hostname_id = 1
msg_item_bindaddr_id = 2
msg_item_rpcaddr_id = 3
msg_item_starthash_id = 4
msg_item_serfnode_id = 5
msg_item_vnode_id = 6


def read_from_stdin():
    payload = ''
    for line in sys.stdin:
        line = line[:line.rindex('\n')]
        payload += line
    return payload


def get_serf_node():
    return os.environ.get('SERF_SELF_NAME', '')


# This function could raises Exception when error happens, so we should catch
# Exception when we call this function
def parse_serf_helper(serf_node, section, item):
    cfg_name = '{0}.evhelper.ini'.format(serf_node)
    cf = ConfigParser.ConfigParser()
    cf.read(cfg_name)
    return cf.get(section, item)


class BinaryMsg(object):

    def __init__(self, cmd, items):
        self.cmd = cmd
        self.bodylen = reduce(
            lambda x, y: x + y, [item.size() for item in items])
        self.extra = 0
        self.items = items

    def size(self):
        return msg_header_size + self.bodylen

    def network_binary(self):
        header = struct.pack("!BhB", self.cmd, self.bodylen, self.extra)
        body = reduce(lambda x, y: x + y,
                      [item.network_binary() for item in self.items])
        return header + body


class BinaryItem(object):

    def __init__(self, itemid, itembody):
        self.itemid = itemid
        if not isinstance(itembody, str):
            itembody = str(itembody)
        self.bodylen = len(itembody)
        self.body = itembody

    def size(self):
        return msg_itemid_size + msg_itemlen_size + self.bodylen

    def network_binary(self):
        pack_fmt = "!Bh{0}s".format(self.bodylen)
        return struct.pack(pack_fmt, self.itemid, self.bodylen, self.body)


class DefaultHandler(SerfHandler):

    def __init__(self):
        super(SerfHandler, self).__init__()
        self.serf_node = get_serf_node()
        self.logfile = "py-serf-master.{}.log".format(self.serf_node)

    def _send_status(self, msg, addr):
        ip, port = addr.split(":")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, int(port)))
        s.send(msg.network_binary())
        s.close()

    def _send_nodeinfo(self, msg_dict, rpc_addr):
        # missing all the following filed will raise Exception
        items = []
        items.append(
            BinaryItem(msg_item_bindaddr_id, msg_dict[serf_msg_bindaddr]))
        items.append(
            BinaryItem(msg_item_hostname_id, msg_dict[serf_msg_hostname]))
        items.append(
            BinaryItem(msg_item_serfnode_id, msg_dict[serf_msg_serf]))
        items.append(
            BinaryItem(msg_item_rpcaddr_id, msg_dict[serf_msg_rpcaddr]))
        items.append(
            BinaryItem(msg_item_starthash_id, msg_dict[serf_msg_starthash]))

        bmsg = BinaryMsg(msg_cmd_nodeinfo, items)
        self._send_status(bmsg, rpc_addr)

    def _send_vnodeinfo(self, msg_dict, rpc_addr):
        # missing all the following filed will raise Exception
        items = []
        items.append(
            BinaryItem(msg_item_hostname_id, msg_dict[serf_msg_hostname]))
        items.append(
            BinaryItem(msg_item_serfnode_id, msg_dict[serf_msg_serf]))
        items.append(
            BinaryItem(msg_item_vnode_id, msg_dict[serf_msg_vnode]))

        bmsg = BinaryMsg(msg_cmd_vnodeinfo, items)
        self._send_status(bmsg, rpc_addr)

    def nodeinfo(self):
        with open(self.logfile, 'a+') as f:
            try:
                rpc_addr = parse_serf_helper(
                    self.serf_node, cfg_sect_node, cfg_item_rpcaddr)
            except (KeyError, ConfigParser.NoSectionError, ConfigParser.NoOptionError):
                f.write(traceback.format_exc())
                return

            payload = read_from_stdin()
            try:
                msg_dict = json.loads(payload)

                # ignore self serf event
                if msg_dict[serf_msg_serf] == get_serf_node():
                    f.write('self nodeinfo event detected, ignore...\n')
                    return

                f.write('nodeinfo event detected...\n')
                f.write('send payload: {0} to {1}\n'.format(payload, rpc_addr))

                self._send_nodeinfo(msg_dict, rpc_addr)

            except Exception:
                f.write(traceback.format_exc())

    def vnodeinfo(self):
        with open(self.logfile, 'a+') as f:
            try:
                rpc_addr = parse_serf_helper(
                    self.serf_node, cfg_sect_node, cfg_item_rpcaddr)
            except (KeyError, ConfigParser.NoSectionError, ConfigParser.NoOptionError):
                f.write(traceback.format_exc())
                return

            payload = read_from_stdin()
            try:
                msg_dict = json.loads(payload)

                # ignore self serf event
                serf_node_name = base64.b64decode(msg_dict[serf_msg_serf])
                if serf_node_name == get_serf_node():
                    f.write('self vnodeinfo event detected, ignore...\n')
                    return

                f.write('vnodeinfo event detected...\n')

                self._send_vnodeinfo(msg_dict, rpc_addr)

            except Exception:
                f.write(traceback.format_exc())

    def member_join(self):
        with open(self.logfile, 'a+') as f:
            try:
                payload = read_from_stdin()
                f.write("member join detected, payload: {}\n".format(payload))
            except Exception:
                f.write(traceback.format_exc())
        # maybe rebalance the load balancer


if __name__ == '__main__':
    handler = SerfHandlerProxy()
    handler.register('default', DefaultHandler())
    handler.run()
