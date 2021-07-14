drop table if exists ods.yw_employee_workday_manage;
create external table ods.yw_employee_workday_manage(
id                                                     int             comment '主键',
employee_id                                            int             comment '员工id',
manager_name                                           string          comment '主管姓名',
manager_id                                             int             comment '主管id',
begin_datetime                                         int             comment '开始时间',
end_datetime                                           int             comment '结束时间',
employee_grade                                         int             comment '咨询师等级',
incustomer_quota                                       int             comment '上户配额',
city_id                                                bigint          comment '城市id',
monday                                                 int             comment '星期一:1工作2休息3请假4代班',
tuesday                                                int             comment '星期二:1工作2休息3请假4代班',
wednesday                                              int             comment '星期三:1工作2休息3请假4代班',
thursday                                               int             comment '星期四:1工作2休息3请假4代班',
friday                                                 int             comment '星期五:1工作2休息3请假4代班',
saturday                                               int             comment '星期六:1工作2休息3请假4代班',
sunday                                                 int             comment '星期日:1工作2休息3请假4代班',
workday_schedue_id                                     int             comment '上户计划表id',
plan_everyday_incustomer                               int             comment '预计每日上户量',
rest_day_num                                           int             comment '休息天数统计',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_workday_manage'
;
