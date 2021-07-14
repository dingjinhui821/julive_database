drop table if exists ods.yw_employee_grade_level;
create external table ods.yw_employee_grade_level(
id                                                     bigint          comment '',
city_id                                                int             comment '城市id',
team_leader_id                                         int             comment '主管id',
department_id                                          int             comment '部门id',
employee_id                                            int             comment '员工id',
job_number                                             string          comment '员工号',
max_level                                              int             comment '最高等级',
quarter_level                                          int             comment '季度等级',
max_level_datetime                                     string          comment '最高等级添加时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
adjust_datetime                                        int             comment '降级执行时间',
grade_log                                              string          comment '更新职级等级日志',
status                                                 int             comment '状态 1正常 2备份',
backup_datetime                                        int             comment '备份时间',
adjust_city_id                                         bigint          comment '核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_grade_level'
;
