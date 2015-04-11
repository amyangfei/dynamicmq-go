## Dispatcher

1. Messages from Matcher to Dispatcher
    * Message binary protocol
        * Message header
            * 1 byte message comamnd type
            * 2 byte message body length
            * 1 byte message extra field
        * Message body contains one or more items, total length equals to body length in header
            * Item header
                * 1 byte item id
                * 2 byte item body length
            * Item body
                * Lenght equals to body length in item header
    * MaxBodyLength 2000
    * ExtraField
        * ExtraSendSingle   0x01
        * ExtraSendMulHead  0x02
        * ExtraSendMulMid   0x04
        * ExtraSendMulTail  0x08
    * Body item id
        * MsgId         1
        * PayloadId     2
        * ClientListId  3

2. Message from one Dispatcher to another Dispatcher
    * This message can be sent to corresponding Connector directly
    * Message binary protocol
        * Message header
            * 1 byte message comamnd type
            * 2 byte message body length
            * 1 byte message extra field
        * Message body contains one or more items, total length equals to body length in header
            * Item header
                * 1 byte item id
                * Lenght equals to body length in item header
    * MaxBodyLength 2000
    * ExtraField
        * ExtraSendSingle   0x01
        * ExtraSendMulHead  0x02
        * ExtraSendMulMid   0x04
        * ExtraSendMulTail  0x08
    * Body item id
        * MsgId         1
        * PayloadId     2
        * ClientListId  3

3. Message from Dispatcher to Connector
    * same as 2
    * command type
        * handshake    = 1 # used for exchanging node information
        * heartbeat    = 2
        * push message = 3


## sdk

* Basic message structure

        #<subsequence message lenght(number of bytes)> CR LF
        *<number of elements> CR LF
        $<number of bytes of element-1> CR LF
        <element-1 data> CR LF
        ...
        $<number of bytes of element-N> CR LF
        <element-N data> CR LF

* command
    * Auth
        * auth "auth information json string"
        * auth information json: {"client_id": clientid, "timestamp": timestamp, "token": token}
    * Heartbeat
        * hb
    * subscribe
        * sub "attr1 name" "attr1 field json string" ... "attrN name" "attrN field json string"
        * attr field json: {"use": 1,  "strval": string-value, "low": low, "high": high, "extra": "can be a json string"}
            * "use": represents which field will be used. 1: strval, 2: low, high, 3: extra
            * "strval": single string value
            * "low","high": both low and high are float, attribute ranges from low to high
            * "extra": a json string for attribute extending

* message from connector to adk
    * Heartbeat reply
        * +h CR LF
    * auth success reply
        * +authsuccess CR LF
    * command error reply
        * -command error CR LF
    * push message

            #<subsequence message length(number of bytes)> CR LF
            <message content json string> CR LF


## Matcher

1. Status message
    * Message binary protocol
        * Message header
            * 1 byte message comamnd type
            * 2 byte message body length
            * 1 byte message extra field
        * Message body contains one or more items, total length equals to body length in header
            * Item header
                * 1 byte item id
                * 2 byte item body length
            * Item body
                * Lenght equals to body length in item header
    * MaxBodyLength 2000
    * ExtraField, not use now
    * Body item id
        * HostnameId  1
        * BindAddrId  2
        * RPCAddrId   3
        * StartHashId 4
        * SerfNodeId  5
        * VnodeListId 6


## Publisher

1. Publish message
    * Message binary protocol
            * 1 byte message comamnd type
            * 2 byte message body length
            * 1 byte message extra field
        * Message body contains one or more items, total length equals to body length in header
            * Item header
                * 1 byte item id
                * 2 byte item body length
            * Item body
                * Lenght equals to body length in item header
    * MaxBodyLength 2000
    * ExtraField, not use now
    * Body item id
        * AttributeId   1
        * PayloadId     2
