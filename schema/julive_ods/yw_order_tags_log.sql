drop table if exists ods.yw_order_tags_log;
create external table ods.yw_order_tags_log(
id                                                     int             comment '',
tag_id                                                 int             comment '标签id',
order_id                                               int             comment '订单id',
action                                                 int             comment '操作:1添加 2删除',
creator                                                int             comment '操作人',
create_datetime                                        int             comment '操作时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_tags_log'
;
