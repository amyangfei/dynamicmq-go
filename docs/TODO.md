## feature require

* auto load datanode topology in indexnode to ensure we don't need to start indexnode before datanode.

## enhancement

* etcd client buffer pool; DONE

* use time-wheel to replace SetReadDeadline in connecting service to improve performance.

## undefined

* In connecting service, whether using a new goroutine for each message sending to sub client.
