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

3. Message from Dispatcher to Connector
    * same as 2
    * command type
        * handshake    = 1 # used for exchanging node information
        * heartbeat    = 2
        * push message = 3

