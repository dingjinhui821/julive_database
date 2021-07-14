drop table if exists ods.market_activity_url;
create external table ods.market_activity_url(
id                                                     int             comment '',
market_activity_id                                     int             comment '活动id',
url                                                    string          comment '活动url',
page_name                                              string          comment '页面名称',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
channel_id                                             string          comment '渠道id',
channel_put                                            string          comment 'channel_put',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_url'
;
