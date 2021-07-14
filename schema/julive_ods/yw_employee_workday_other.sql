drop table if exists ods.yw_employee_workday_other;
create external table ods.yw_employee_workday_other(
id                                                     int             comment '主键',
workday_manage_id                                      int             comment '排班管理表id',
substitute_workday_manage_id                           int             comment '代班人manage_id',
substitute_employee_id                                 int             comment '代班人id',
been_substitute_employee_id                            int             comment '被代人id',
incustomer_quota                                       int             comment '转移过来的配额',
week_day                                               string          comment '当前周几 存管理表字段',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_workday_other'
;
