drop table if exists ods.yw_consult_goal;
create external table ods.yw_consult_goal(
id                                                     bigint          comment '',
employee_id                                            bigint          comment '咨询师id',
employee_name                                          string          comment '咨询师姓名',
employee_leader_id                                     bigint          comment '咨询师当前主管id',
employee_leader_name                                   string          comment '咨询师当前主管姓名',
employee_leader_month_id                               bigint          comment '咨询师当月主管id',
employee_leader_month_name                             string          comment '咨询师当月主管姓名',
incustomer_num                                         int             comment '上户量',
see_project_num                                        int             comment '带看量',
subscribe_num                                          int             comment '认购量',
sign_num                                               int             comment '网签量',
city_id                                                int             comment '城市id',
datetime                                               int             comment '目标时间',
create_datetime                                        int             comment '添加时间',
creator                                                bigint          comment '添加人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_consult_goal'
;
