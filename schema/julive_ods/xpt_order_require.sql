drop table if exists ods.xpt_order_require;
create external table ods.xpt_order_require(
id                                                     int             comment '自增id',
order_id                                               int             comment '订单id',
user_id                                                int             comment '用户id',
require_type                                           int             comment '需求类型 0 不限 1 新房 2 二手房',
house_type                                             string          comment '户型，逗号分隔',
acreage_min                                            int             comment '最小面积',
acreage_max                                            int             comment '最大面积',
total_price_min                                        int             comment '最小总价',
total_price_max                                        int             comment '最大总价',
first_price_min                                        int             comment '最少首付',
first_price_max                                        int             comment '最多首付',
remark                                                 string          comment '备注',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order_require'
;
