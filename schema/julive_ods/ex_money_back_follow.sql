drop table if exists ods.ex_money_back_follow;
create external table ods.ex_money_back_follow(
id                                                     int             comment 'id',
group_id                                               int             comment '回款组id',
follow_datetime                                        int             comment '实际跟进时间',
plan_pay_datetime                                      int             comment '预计回款时间',
employee_id                                            int             comment '本次跟进人',
follow_text                                            string          comment '跟进说明',
next_follow_datetime                                   int             comment '下次跟进时间',
next_employee_id                                       int             comment '下次跟进人',
audit_status                                           int             comment '跟进审核状态 1.待审核2.审核通过3.审核驳回',
audit_employee_id                                      int             comment '审核人',
audit_datetime                                         int             comment '审核时间',
reject_text                                            string          comment '驳回原因',
start_stage_type                                       int             comment '开始跟进阶段',
back_stage_type                                        int             comment '回退的阶段',
target_stage_type                                      int             comment '目标的阶段',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_money_back_follow'
;
