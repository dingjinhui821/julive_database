drop table if exists ods.market_activity_project;
create external table ods.market_activity_project(
id                                                     int             comment '',
market_activity_id                                     int             comment '活动id',
project_id                                             bigint          comment '楼盘id',
pay_points                                             string          comment '利益点',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_project'
;
