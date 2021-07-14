drop table if exists ods.yw_order_customer;
create external table ods.yw_order_customer(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
name                                                   string          comment '用户名',
mobile                                                 string          comment '用户手机号，多个用逗号隔开',
creator                                                bigint          comment '',
create_datetime                                        int             comment '',
updator                                                bigint          comment '',
update_datetime                                        int             comment '',
status                                                 int             comment '状态，1正常，-1逻辑删除',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_customer'
;
