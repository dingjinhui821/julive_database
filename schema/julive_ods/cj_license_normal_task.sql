drop table if exists ods.cj_license_normal_task;
create external table ods.cj_license_normal_task(
id                                                     bigint          comment 'id',
license_id                                             int             comment '许可证id',
data_type                                              int             comment '数据类型 1抓取 2居理',
task_update_datetime                                   int             comment '任务更新时间',
close_datetime                                         int             comment '关闭时间',
close_type                                             int             comment '关闭类型 1系统关闭 2运营关闭',
close_employee_id                                      int             comment '关闭人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license_normal_task'
;
