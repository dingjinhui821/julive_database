drop table if exists ods.cj_license_task_project;
create external table ods.cj_license_task_project(
id                                                     bigint          comment 'id',
task_id                                                bigint          comment '许可证任务id',
cj_project_id                                          int             comment '居理楼盘id',
create_datetime                                        int             comment '更新时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_task_project'
;
