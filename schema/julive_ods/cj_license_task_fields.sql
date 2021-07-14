drop table if exists ods.cj_license_task_fields;
create external table ods.cj_license_task_fields(
id                                                     bigint          comment 'id',
task_id                                                bigint          comment '许可证任务id',
field_id                                               int             comment '字段id',
field_value                                            string          comment '字段值信息',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_task_fields'
;
