drop table if exists ods.adjust_see_project_employee_detail;
create external table ods.adjust_see_project_employee_detail(
see_project_id                                         bigint          comment '带看id',
employee_id                                            bigint          comment '咨询师id',
employee_adjust_city                                   bigint          comment '咨询师核算城市',
manager_id                                             bigint          comment '主管id',
manager_adjust_city                                    bigint          comment '主管核算城市',
manager_leader_id                                      bigint          comment '咨询经理id',
manager_leader_adjust_city                             bigint          comment '经理核算城市',
value                                                  double          comment '核算量(最高1)',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
happen_updatetime                                      int             comment '业务发生时间',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
id                                                     bigint          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/adjust_see_project_employee_detail'
;
