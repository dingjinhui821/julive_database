drop table if exists ods.yw_employee_executive_day;
create external table ods.yw_employee_executive_day(
id                                                     bigint          comment '',
employee_id                                            int             comment '员工id',
employee_manage_id                                     int             comment '员工主管id',
department_id                                          int             comment '部门id',
city_id                                                int             comment '城市id',
deduct_score                                           int             comment '扣除分数',
tag_config_id                                          int             comment '标签对应的表配置',
calculate_time                                         int             comment '计算执行力时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
tag_day                                                int             comment '标签时间',
count_nums                                             int             comment '本次扣除次数',
deduct_score_log                                       string          comment '扣除分数日志',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_executive_day'
;
