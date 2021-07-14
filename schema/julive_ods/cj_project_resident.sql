drop table if exists ods.cj_project_resident;
create external table ods.cj_project_resident(
project_id                                             int             comment '楼盘id',
resident_employee_id                                   int             comment '驻场人员id',
city_id                                                int             comment '城市id',
resident_status                                        int             comment '驻场状态 1驻场中 2停止驻场',
start_datetime                                         int             comment '进场时间',
end_datetime                                           int             comment '离场时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
note                                                   string          comment '修改说明',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_resident'
;
