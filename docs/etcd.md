## connector information
    * single connector information
        * /conn/info/<node-id>/status
        * /conn/info/<node-id>/sub_addr
        * /conn/info/<node-id>/route_addr
        * /conn/info/<node-id>/capacity
        * /conn/info/<node-id>/load
        * /conn/info/<conn-node-id>/sub/<sub-node-id>
        * /conn/info/<node-id>/disp_id, not used

    * connector that is waiting to be connected by dispatcher
        * a series of dirs represent unassociated connectors
        * /conn/waiting/<node-id>/


## dispatcher information
    * single dispatcher information
        * /disp/info/<node-id>/status
        * /disp/info/<node-id>/conn_id
        * /disp/info/<node-id>/match_addr

## subscriber(sdk) information
    * single subscriber information
        * /sub/info/<client-id>/conn_id
        * subscription, key: /sub/attr/<client-id>/<attribute-name> value:<description-json-string>
        * more attributes ...

## index base information
    * attribute dimension
        * /idx/info/dimension
    * each dimension attribute's lower and upper bound
        * /idx/info/bound/<attr-name>/lower
        * /idx/info/bound/<attr-name>/upper

## datanode chord information
    * datanode
        * /datn/pnode/<node-id>/status new/active/offline
        * /datn/pnode/<node-id>/pub_addr

    * vnode
        * /datn/vnode/<vnode-hash> node-id
