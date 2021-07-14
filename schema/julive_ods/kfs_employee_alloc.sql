drop table if exists ods.kfs_employee_alloc;
create external table ods.kfs_employee_alloc(
id                                                     int             comment '主键id',
employee_id                                            int             comment '员工id',
alloc_status                                           int             comment '接单状态 1:接单 2:不接单',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_employee_alloc'
;
