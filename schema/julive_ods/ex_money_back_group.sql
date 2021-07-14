drop table if exists ods.ex_money_back_group;
create external table ods.ex_money_back_group(
id                                                     int             comment 'id',
follow_datetime                                        int             comment '最新跟进时间',
pay_datetime                                           int             comment '回款完成时间',
plan_pay_datetime                                      int             comment '预计回款时间',
next_follow_datetime                                   int             comment '下次跟进时间',
stage_type                                             int             comment '当前阶段',
follow_employee_id                                     int             comment '最新跟进人',
ex_money_back_config_id                                int             comment '回款流程id',
original_goup_id                                       int             comment '原回款组id（移除生成前id）',
delete_status                                          int             comment '删除状态 1是2否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_money_back_group'
;
