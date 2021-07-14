drop table if exists ods.yw_employee_workday_city_config;
create external table ods.yw_employee_workday_city_config(
id                                                     int             comment '主键',
city_id                                                bigint          comment '城市id',
explosive_value                                        int             comment '爆单率',
start_work_time                                        string          comment '上班时间',
lunch_break_time                                       string          comment '午休时间',
close_work_time                                        string          comment '下班时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_workday_city_config'
;
