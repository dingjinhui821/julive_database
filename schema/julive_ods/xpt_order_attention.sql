drop table if exists ods.xpt_order_attention;
create external table ods.xpt_order_attention(
id                                                     int             comment 'id',
order_id                                               int             comment '订单id',
area_id                                                int             comment '区域id',
circle_id                                              int             comment '商圈id',
type                                                   int             comment '关注类型 1 区域 2 商圈',
require_type                                           int             comment '关注类型 2 新房 3 二手房,',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order_attention'
;
