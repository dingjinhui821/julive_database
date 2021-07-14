drop table if exists ods.xpt_order_attr;
create external table ods.xpt_order_attr(
id                                                     int             comment '自增id',
order_id                                               int             comment '订单id',
source_order_id                                        int             comment '来源订单id 例如 平台给客 为新房订单id',
kefu_id                                                int             comment '客服id',
source_order_status                                    int             comment '来源订单的状态',
source_order_follow                                    int             comment '不源订单跟进时间',
require_type                                           int             comment '关注类型  1 不限 2 新房 3 二手房',
from_page                                              int             comment '留电来源页面',
op_type                                                int             comment '留电口',
track_id                                               int             comment '埋点id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
comjia_platform_id                                     int             comment 'comjia_platform_id',
product_id                                             int             comment 'product_id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order_attr'
;
