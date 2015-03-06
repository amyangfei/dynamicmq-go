#!/usr/bin/env python

from serf_master import SerfHandler, SerfHandlerProxy
import ConfigParser
import sys
import os


cfg_sect_node    = "node"
cfg_item_rpcaddr = "rpc_addr"


def read_from_stdin():
    payload = ''
    for line in sys.stdin:
        line = line[:line.rindex('\n')]
        payload += line
    return payload


def get_serf_node():
    return os.environ.get('SERF_SELF_NAME', '')


# This function should raises Exception when error happens, so we should catch
# Exception when we call this function
def parse_serf_helper(serf_node, section, item):
    cfg_name = '{0}.evhelper.ini'.format(serf_node)
    cf = ConfigParser.ConfigParser()
    cf.read(cfg_name)
    return cf.get(section, item)


class DefaultHandler(SerfHandler):
    def nodeinfo(self):
        serf_node = get_serf_node()
        try:
            rpc_addr = parse_serf_helper(
                    serf_node, cfg_sect_node, cfg_item_rpcaddr)
        except (KeyError, ConfigParser.NoSectionError,ConfigParser.NoOptionError) as e:
            print e
        with open('py-serf-master.log', 'a+') as f:
            f.write('nodeinfo event detected...\n')
            payload = read_from_stdin()
            f.write('send payload: {0} to {1}'.format(payload, rpc_addr))
            f.write('\n')

    def vnodeinfo(self):
        with open('py-serf-master.log', 'a+') as f:
            f.write('vnodeinfo event detected...\n')
            for line in sys.stdin:
                line = line[:line.rindex('\n')-1]
                f.write(line)
                f.write('\n')

    def member_join(self):
        pass
        # maybe rebalance the load balancer


if __name__ == '__main__':
    handler = SerfHandlerProxy()
    handler.register('default', DefaultHandler())
    handler.run()
