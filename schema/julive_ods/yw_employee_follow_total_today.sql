drop table if exists ods.yw_employee_follow_total_today;
create external table ods.yw_employee_follow_total_today(
id                                                     bigint          comment '',
employee_id                                            bigint          comment '',
employee_name                                          string          comment '咨询师名称',
employee_manage_id                                     int             comment '咨询师当前主管id',
employee_manage_name                                   string          comment '咨询师当前主管名称',
city_id                                                int             comment '城市id',
do_time                                                int             comment '日期',
follow_order_total                                     int             comment '当天咨询师订单开关为开的非网签状态订单量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
city_name                                              string          comment '城市名称',
plan_contact                                           int             comment '当天预约联系数',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_follow_total_today'
;
