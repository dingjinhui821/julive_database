drop table if exists ods.market_activity_channel;
create external table ods.market_activity_channel(
id                                                     int             comment '',
market_activity_id                                     int             comment '活动id',
channel_id                                             int             comment '渠道id',
channel_name                                           string          comment '渠道名称',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_channel'
;
