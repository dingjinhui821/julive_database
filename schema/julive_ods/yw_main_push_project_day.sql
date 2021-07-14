drop table if exists ods.yw_main_push_project_day;
create external table ods.yw_main_push_project_day(
id                                                     bigint          comment '',
creator                                                bigint          comment '',
create_datetime                                        bigint          comment '',
updator                                                bigint          comment '',
update_datetime                                        bigint          comment '',
project_id                                             bigint          comment '楼盘id',
expect_num                                             int             comment '预计本周成交套数',
expect_money                                           int             comment '预计本周套佣',
expect_total                                           int             comment '预计本周总套佣',
city_id                                                int             comment '城市id',
real_num                                               int             comment '实际本周成交套数',
real_total                                             double          comment '实际本周总套佣',
real_money                                             double          comment '实际本周套佣',
house_change_desc                                      string          comment '房源变动概述',
datetime                                               bigint          comment '日开始时间戳',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_main_push_project_day'
;
