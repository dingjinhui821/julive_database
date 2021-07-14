drop table if exists ods.market_activity_rules;
create external table ods.market_activity_rules(
id                                                     int             comment '',
market_activity_id                                     int             comment '活动id',
price_max                                              double          comment '最高总价',
price_min                                              double          comment '最低总价',
district_id                                            bigint          comment '区域id',
project_type                                           bigint          comment '业态',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
start_datetime                                         int             comment '规则开始时间',
end_datetime                                           int             comment '规则截止时间',
info                                                   string          comment '规则备注',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_rules'
;
