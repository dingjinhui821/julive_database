drop table if exists ods.adjust_sign_employee_detail;
create external table ods.adjust_sign_employee_detail(
sign_id                                                bigint          comment '签约id',
employee_id                                            bigint          comment '咨询师id',
manager_id                                             bigint          comment '主管id',
manager_leader_id                                      bigint          comment '咨询经理id',
value                                                  double          comment '核算量(最高1)',
happen_updatetime                                      int             comment '业务发生时间',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
adjust_type                                            int             comment '核算类型:1签约服务人',
id                                                     bigint          comment '',
employee_adjust_city                                   bigint          comment '咨询师核算城市',
manager_adjust_city                                    bigint          comment '主管核算城市',
manager_leader_adjust_city                             bigint          comment '经理核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_sign_employee_detail'
;
