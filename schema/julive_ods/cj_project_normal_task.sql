drop table if exists ods.cj_project_normal_task;
create external table ods.cj_project_normal_task(
id                                                     bigint          comment 'id',
project_id                                             int             comment '楼盘id',
house_type_id                                          int             comment '户型id',
building_id                                            int             comment '楼栋id',
mod_type                                               int             comment '模块类型 1基本信息 2户型信息 3楼栋信息 4点评 5动态 6图册',
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
location '/dw/ods/cj_project_normal_task'
;
