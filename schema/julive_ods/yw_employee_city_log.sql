drop table if exists ods.yw_employee_city_log;
create external table ods.yw_employee_city_log(
id                                                     int             comment '主键id',
employee_id                                            int             comment '咨询师id',
city_arr                                               string          comment '设置的可接单城市群',
set_type                                               int             comment '设置类型（1.单独设置，2.批量设置）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_city_log'
;
