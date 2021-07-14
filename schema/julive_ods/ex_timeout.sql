drop table if exists ods.ex_timeout;
create external table ods.ex_timeout(
id                                                     int             comment '',
business_id                                            int             comment '业务id',
business_type                                          int             comment '业务类型 1模拟邀约 2联系 3模拟谈判 4见面',
ex_order_id                                            int             comment 'bd单id',
plan_datetime                                          int             comment '预约时间',
real_datetime                                          int             comment '实际录入时间',
employee_id                                            int             comment '拓展员工id',
delay_time                                             int             comment '延迟时长',
is_delay                                               int             comment '是否延迟 1是 2否',
leader_id                                              int             comment '主管id',
mark_type                                              int             comment '标记超时类型:0还未标记，1计划任务自动标记，2发生实际联系触发标记',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_timeout'
;
