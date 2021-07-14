drop table if exists ods.adjust_subscribe_employee_detail;
create external table ods.adjust_subscribe_employee_detail(
subscribe_id                                           bigint          comment '认购id',
employee_id                                            bigint          comment '咨询师id',
manager_id                                             bigint          comment '主管id',
value                                                  double          comment '核算量(最高1)',
happen_updatetime                                      int             comment '业务发生时间',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
employee_adjust_city                                   bigint          comment '咨询师核算城市',
manager_adjust_city                                    bigint          comment '主管核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_subscribe_employee_detail'
;
