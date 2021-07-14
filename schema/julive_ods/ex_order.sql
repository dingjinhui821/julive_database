drop table if exists ods.ex_order;
create external table ods.ex_order(
id                                                     int             comment 'bd单id',
city_id                                                int             comment '城市id',
follow_employee_id                                     int             comment '跟进人id',
cooperate_type                                         int             comment '合作方式 1开发商直签 2平台合作 3战略合作',
partner_id                                             string          comment '合作方公司id，逗号分隔',
status                                                 int             comment '1项目学习，2寻人，3邀约，4谈判，5条款，6草签，7网签',
decision_employee_id                                   int             comment '决策人id（开发商员工）',
is_market                                              int             comment '决策人是否是营销人 1是 2否',
market_employee_id                                     int             comment '营销负责人id（开发商员工）',
history_follow_employee                                string          comment '历史跟进人，逗号分隔',
is_confirm_distribute                                  int             comment '是否确认收到分配 1是 2否',
confirm_distribute_datetime                            int             comment '确认收到分配时间',
follow_status                                          int             comment '跟进状态 1正常 2停止跟进',
stop_follow_note                                       string          comment '停止跟进理由',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
old_bd_id                                              int             comment '旧的bd单id',
change_follow_note                                     string          comment '修改跟进人理由',
finish_learn_datetime                                  int             comment '项目学习完成时间',
find_people_sys_datetime                               int             comment '系统规定完成寻人时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_order'
;
