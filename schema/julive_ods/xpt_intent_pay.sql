drop table if exists ods.xpt_intent_pay;
create external table ods.xpt_intent_pay(
id                                                     int             comment '主键id',
report_id                                              int             comment '报备单id',
order_id                                               int             comment '订单id',
user_name                                              string          comment '客户姓名',
mobile                                                 string          comment '客户电话',
status                                                 int             comment '支付状态 1成功，2失败',
pay_time                                               int             comment '支付时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_intent_pay'
;
