drop table if exists ods.cj_project_resident_employee;
create external table ods.cj_project_resident_employee(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
employee_id                                            int             comment '驻场人员id',
status                                                 int             comment '驻场状态 1驻场中 2停止驻场',
start_datetime                                         int             comment '进场时间',
end_datetime                                           int             comment '离场时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_resident_employee'
;
