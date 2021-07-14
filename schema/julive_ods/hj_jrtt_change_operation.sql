drop table if exists ods.hj_jrtt_change_operation;
create external table ods.hj_jrtt_change_operation(
id                                                     int             comment '主键自增',
change_mode                                            int             comment '方式 1 跟踪应用下载',
device                                                 int             comment '设备 101 android 201 ios',
pack_task_id                                           int             comment 'yw_android_pack主id',
change_target                                          int             comment '转化目标 1 激活',
operation_status                                       int             comment '状态 1未关联 2已关联未上传 3已上传 4上传失败 5上传中-展示已关联未上传',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_jrtt_change_operation'
;
