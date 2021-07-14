drop table if exists ods.kfs_employee_project_relation;
create external table ods.kfs_employee_project_relation(
id                                                     int             comment '主键',
employee_id                                            int             comment '员工id',
project_id                                             bigint          comment '楼盘id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
city_id                                                bigint          comment '城市',
is_delete                                              int             comment '是否删除:1否2是',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_employee_project_relation'
;
