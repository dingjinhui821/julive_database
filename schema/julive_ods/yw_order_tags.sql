drop table if exists ods.yw_order_tags;
create external table ods.yw_order_tags(
id                                                     int             comment '',
tag_id                                                 int             comment '标签id',
order_id                                               int             comment '订单id',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_tags'
;
