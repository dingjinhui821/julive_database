drop table if exists ods.yw_pay_follow;
create external table ods.yw_pay_follow(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
plan_datetime                                          bigint          comment '计划跟进时间',
real_datetime                                          bigint          comment '实际跟进时间',
plan_to_real_datetime                                  int             comment '预约转实际时间',
plan_content                                           string          comment '计划跟进内容',
real_content                                           string          comment '实际跟进内容',
plan_employee_id                                       int             comment '计划跟进咨询师',
real_employee_id                                       int             comment '实际跟进咨询师',
real_desc                                              string          comment '实际跟进说明',
status                                                 int             comment '0:计划跟进 1:实际跟进',
create_datetime                                        bigint          comment '',
creator                                                int             comment '',
update_datetime                                        bigint          comment '',
updator                                                int             comment '',
sign_risk_id                                           bigint          comment '签约风险id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_pay_follow'
;
