drop table if exists ods.cj_abtest_experiment;
create external table ods.cj_abtest_experiment(
id                                                     int             comment '主键',
experiment_id                                          string          comment '实验变量id',
name                                                   string          comment '实验变量名',
`desc`                                                 string          comment '实验描述',
device_type                                            int             comment '设备端 1:pc，2:m，3:app',
type                                                   int             comment '实验类型 1:编程实验，2:多链接实验',
status                                                 int             comment '状态 1:运行中，2:停止',
status_manual                                          int             comment 'cms中实验状态 1:运行中，2:停止',
final_version                                          int             comment '最终版本 0:original_version，1:v1 ...',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_abtest_experiment'
;
