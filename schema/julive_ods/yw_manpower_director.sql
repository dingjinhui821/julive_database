drop table if exists ods.yw_manpower_director;
create external table ods.yw_manpower_director(
id                                                     int             comment '主键id',
city_id                                                int             comment '城市id',
employee_name                                          string          comment '员工姓名',
job_number                                             bigint          comment '员工工号',
employee_id                                            int             comment '员工id',
team_leader_id                                         int             comment '主管id',
need_attendance_days                                   double          comment '应出勤天数',
actual_attendance_days                                 double          comment '实出勤天数(打卡 - 旷工)',
need_check_days                                        double          comment '打卡天数',
leave_work_days                                        double          comment '旷工天数',
man_power                                              double          comment '人力数(实出勤/应出勤)',
entry_datetime                                         int             comment '入职时间',
offjob_datetime                                        int             comment '离职时间',
employee_status                                        int             comment '员工状态:0离职 1在职 2停访(核算月最后一天)',
adjust_datetime                                        int             comment '核算时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
calculation_log                                        string          comment '计算日志',
adjust_city_id                                         bigint          comment '核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_manpower_director'
;
