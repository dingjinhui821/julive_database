drop table if exists ods.yw_employee_seat;
create external table ods.yw_employee_seat(
id                                                     int             comment '主键id',
cno                                                    string          comment '坐席号',
employee_id                                            int             comment '支撑系统的员工id--客服id',
employee_job_number                                    string          comment '支撑系统的员工工号--客服工号',
employee_name                                          string          comment '支撑系统的员工名称--客服名称',
active                                                 int             comment '是否激活，1是2否',
city_id                                                int             comment '城市id',
city_name                                              string          comment '城市名称',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
ag_id                                                  int             comment '坐席id',
sobot_service_id                                       string          comment '智齿客服id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_seat'
;
