## bug fix

- [ ] race condition in indexnode. see [issue #5](https://github.com/amyangfei/dynamicmq-go/issues/5)

## feature require

- [ ] auto load datanode topology in indexnode to ensure we don't need to start indexnode before datanode.

## enhancement

- [X] etcd client buffer pool

- [ ] use time-wheel to replace SetReadDeadline in connecting service to improve performance.

- [ ] new attribute notification mechanism. see [issue #4](https://github.com/amyangfei/dynamicmq-go/issues/4)

* Project infrastructure

    - [ ] support http://goreportcard.com/ for code style
    - [ ] godoc support
    - [ ] travis-ci support

## undefined

- [ ] In connecting service, whether using a new goroutine for each message sending to sub client.
