drop table if exists ods.kfs_manager_relation;
create external table ods.kfs_manager_relation(
id                                                     int             comment '主键id',
manager_type                                           int             comment '上级类型（1.主管）',
manager_id                                             int             comment '主管id',
employee_id                                            int             comment '咨询师id',
project_id                                             bigint          comment '楼盘id',
is_delete                                              int             comment '是否删除 1.否 2.是',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_manager_relation'
;
