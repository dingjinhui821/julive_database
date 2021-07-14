drop table if exists ods.market_activity_plan;
create external table ods.market_activity_plan(
id                                                     int             comment '',
market_activity_id                                     int             comment '活动id',
action                                                 string          comment '关键动作',
target                                                 string          comment '动作目标',
end_datetime                                           int             comment '时间节点（该条动作的截止时间）',
employee_ids                                           string          comment '负责人(可以多个)',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_plan'
;
