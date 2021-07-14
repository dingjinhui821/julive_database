drop table if exists ods.alloc_plan_date_schedule;
create external table ods.alloc_plan_date_schedule(
id                                                     int             comment '自增id',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '修改时间',
updator                                                int             comment '修改人',
schedule_id                                            int             comment '计划表id',
date                                                   string          comment '日期',
plan_amount                                            int             comment '计划上户量',
real_amount                                            int             comment '实际上户量',
plan_clue                                              int             comment '计划线索量',
real_clue                                              int             comment '实际线索量',
plan_progress                                          double          comment '计划进度',
real_progress                                          double          comment '实际进度',
close_amount                                           int             comment '当天关闭的上户量',
plan_ratio                                             double          comment '计划上户率',
real_ratio                                             double          comment '实际上户率',
update_reason                                          string          comment '修改原因',
note                                                   string          comment '备注',
city_id                                                int             comment '城市',
manual_flag                                            int             comment '手动修改标记，默认0:非手动,1:手动修改',
avg_accept                                             double          comment '人均接上户',
open_header_num                                        int             comment '当天开过上户的咨询师人数',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/alloc_plan_date_schedule'
;
