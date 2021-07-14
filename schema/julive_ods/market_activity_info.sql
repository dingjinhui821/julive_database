drop table if exists ods.market_activity_info;
create external table ods.market_activity_info(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
title                                                  string          comment '活动标题',
start_datetime                                         int             comment '活动开始时间',
end_datetime                                           int             comment '活动结束时间',
target                                                 string          comment '活动目标',
employee_ids                                           string          comment '活动成员',
employee_names                                         string          comment '咨询师名字',
status                                                 int             comment '状态:1,有效，2删除',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_info'
;
