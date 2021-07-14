drop table if exists ods.esf_order_sell_distribute;
create external table ods.esf_order_sell_distribute(
id                                                     int             comment 'id',
city_id                                                int             comment '订单城市id',
order_id                                               bigint          comment '二手房卖方订单id',
server_employee_id                                     bigint          comment '所属客服id',
alloc_server_employee_id                               bigint          comment '上户客服id',
server_distribute_datetime                             int             comment '分配客服时间',
employee_distribute_datetime                           int             comment '分配咨询师时间',
not_distribute_reason_one                              int             comment '一级原因',
not_distribute_reason_two                              int             comment '二级原因',
employee_id                                            bigint          comment '分配咨询师id',
maybe_customer                                         int             comment '是否有机会上户，1是，2否，0默认',
is_distribute_employee                                 int             comment '是否分配咨询师 1是 2否',
distribute_server_status                               int             comment '分配客服状态，0不分配（默认），1 待分配，2 已分配',
hope_contact_type                                      int             comment '客户期望联系类型，1 正常联系，2 稍后联系',
hope_contact_time                                      int             comment '客服 客户期望联系时间',
order_status                                           int             comment '订单状态10未分配客服、20未分配咨询师、30未联系、40无效线索',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_order_sell_distribute'
;
