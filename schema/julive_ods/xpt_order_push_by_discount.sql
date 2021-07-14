drop table if exists ods.xpt_order_push_by_discount;
create external table ods.xpt_order_push_by_discount(
id                                                     int             comment '自增id',
user_id                                                int             comment '用户id',
mobile                                                 string          comment '用户手机号',
project_id                                             int             comment '楼盘id',
product_id                                             int             comment '系统id',
order_note                                             string          comment '备注',
pay_time                                               int             comment '支付时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order_push_by_discount'
;
