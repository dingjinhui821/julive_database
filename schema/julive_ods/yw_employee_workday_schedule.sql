drop table if exists ods.yw_employee_workday_schedule;
create external table ods.yw_employee_workday_schedule(
id                                                     int             comment '主键',
city_id                                                bigint          comment '城市id',
begin_datetime                                         int             comment '开始时间',
end_datetime                                           int             comment '结束时间',
explosive_value                                        int             comment '爆单值',
monday_plan_incostomer                                 int             comment '周一计划上户量',
tuesday_plan_incostomer                                int             comment '周二计划上户量',
wednesday_plan_incostomer                              int             comment '周三计划上户量',
thursday_plan_incostomer                               int             comment '周四计划上户量',
friday_plan_incostomer                                 int             comment '周五计划上户量',
saturday_plan_incostomer                               int             comment '周六计划上户量',
sunday_plan_incostomer                                 int             comment '周日计划上户量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_workday_schedule'
;
