drop table if exists ods.yw_employee_city;
create external table ods.yw_employee_city(
id                                                     int             comment '',
employee_id                                            int             comment '员工id',
city_id                                                int             comment '城市id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_city'
;
