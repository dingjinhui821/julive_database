drop table if exists ods.yw_employee_phone_recents;
create external table ods.yw_employee_phone_recents(
id                                                     int             comment '自增id',
name                                                   string          comment '备注姓名',
date                                                   string          comment '通话日期',
duration                                               int             comment '通话时长',
type                                                   int             comment '1:被叫,2:主叫,3:未接通',
number                                                 string          comment '对方号码',
number_format                                          string          comment '格式化后的号码',
last_modify                                            string          comment '与该号码最后一次通话时间',
device_id                                              string          comment '设备id',
number_self                                            string          comment '自己号码',
employee_id                                            bigint          comment '员工id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
unite_order_id                                         bigint          comment '统一订单id',
order_id                                               bigint          comment '订单id',
esf_buy_order_id                                       bigint          comment '二手房买方订单id',
esf_sale_order_id                                      bigint          comment '二手房卖方订单id',
recorder_id                                            string          comment '记录id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_phone_recents'
;
