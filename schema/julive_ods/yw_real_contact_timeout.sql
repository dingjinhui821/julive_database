drop table if exists ods.yw_real_contact_timeout;
create external table ods.yw_real_contact_timeout(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               int             comment '订单id',
contact_id                                             int             comment '联系id',
employee_id                                            bigint          comment '员工id',
employee_realname                                      string          comment '员工姓名',
plan_datetime                                          int             comment '预约时间',
real_datetime                                          int             comment '实际录入系统时间',
delayed_time                                           int             comment '已超时时长',
is_cancel                                              int             comment '是否取消:0不是，1是',
employee_leader_id                                     bigint          comment '咨询师主管id',
employee_leader_realname                               string          comment '咨询师主管姓名',
mark_type                                              int             comment '标记超时类型:0还未标记，1计划任务自动标记，2发生实际联系触发标记',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_real_contact_timeout'
;
