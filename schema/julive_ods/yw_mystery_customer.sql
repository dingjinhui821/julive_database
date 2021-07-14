drop table if exists ods.yw_mystery_customer;
create external table ods.yw_mystery_customer(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
mobile                                                 string          comment '手机号',
do_status                                              int             comment '分配状态:1未分配 2已分配 3分配失败',
employee_ids                                           string          comment '咨询师id',
do_datetime                                            int             comment '分配时间',
status                                                 int             comment '状态:1显示 2隐藏',
creator                                                int             comment '操作人',
create_datetime                                        int             comment '操作时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
order_employee_id                                      int             comment '订单分配咨询师',
order_id                                               bigint          comment '订单id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_mystery_customer'
;
