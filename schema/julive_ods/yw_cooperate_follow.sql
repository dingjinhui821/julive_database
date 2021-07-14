drop table if exists ods.yw_cooperate_follow;
create external table ods.yw_cooperate_follow(
id                                                     bigint          comment '',
bd_order_id                                            bigint          comment '项目id',
plan_datetime                                          bigint          comment '计划推进时间',
real_datetime                                          bigint          comment '实际推动时间',
plan_text                                              string          comment '计划推进动作',
real_text                                              string          comment '实际推动结果',
real_desc                                              string          comment '实际情况描述',
attend_people                                          string          comment '参与人',
contact_way                                            int             comment '沟通方式 1:陌拜 2:电话沟通 3:独立拜访 4:陪同拜访 5:经理拜访 6:微信邮件沟通',
follow_employee_id                                     bigint          comment '跟进人，可以是以下三种角色商务专员、拓展专员、拓展部主管',
status                                                 int             comment '推进状态 1:计划推进 2:实际推进',
is_delete                                              int             comment '是否删除 0:未删除 1:已删除',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        bigint          comment '',
update_datetime                                        bigint          comment '',
attendant                                              string          comment '陪同人',
plan_result                                            string          comment '预计结果',
bd_order_status                                        int             comment '状态，1陌拜，2约访，3谈判阶段，4条款阶款，5签约合作',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_cooperate_follow'
;
