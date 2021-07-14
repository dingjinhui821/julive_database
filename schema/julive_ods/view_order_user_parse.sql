drop table if exists ods.view_order_user_parse;
create external table ods.view_order_user_parse(
id                                                     bigint          comment '',
user_mobile_prefix_7                                   string          comment '',
user_id                                                bigint          comment '用户id',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/view_order_user_parse'
;
