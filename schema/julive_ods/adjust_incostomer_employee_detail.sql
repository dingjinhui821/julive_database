drop table if exists ods.adjust_incostomer_employee_detail;
create external table ods.adjust_incostomer_employee_detail(
order_employee_id                                      string          comment '主键id:拼接规则:订单id与咨询师id用_下划线拼接',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '咨询师id',
employee_adjust_city                                   bigint          comment '咨询师核算城市',
manager_id                                             bigint          comment '主管id',
manager_adjust_city                                    bigint          comment '主管核算城市',
value                                                  double          comment '核算量(最高1)',
create_datetime                                        int             comment '',
happen_updatetime                                      int             comment '业务发生时间',
update_datetime                                        int             comment '',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_incostomer_employee_detail'
;
