## connector information
    * single connector information
        * /connector/info/<node-id>/status
        * /connector/info/<node-id>/sub_addr
        * /connector/info/<node-id>/route_addr
        * /connector/info/<node-id>/capacity
        * /connector/info/<node-id>/load
        * /connector/info/<node-id>/disp_id, not used

    * connector that is waiting to be connected by dispatcher
        * a series of dirs represent unassociated connectors
        * /connector/waiting/<node-id>/


## dispatcher information
    * single dispatcher information
        * /dispatcher/info/<node-id>/status
        * /dispatcher/info/<node-id>/conn_id
        * /dispatcher/info/<node-id>/match_addr

## subscriber(sdk) information
    * single subscriber information
        * /sub/info/<client-id>/conn_id
        * key:/sub/info/<client-id>/attribute/<attribute-name> value:<description-json-string>
        * more attributes ...
