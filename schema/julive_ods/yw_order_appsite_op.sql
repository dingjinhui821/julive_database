drop table if exists ods.yw_order_appsite_op;
create external table ods.yw_order_appsite_op(
id                                                     bigint          comment '主键id',
order_id                                               bigint          comment '订单id',
user_id                                                string          comment '用户id串',
create_datetime                                        int             comment '创建时间',
op_type                                                int             comment '操作类型',
update_datetime                                        int             comment '更新时间',
app_id                                                 int             comment 'appid',
channel_id                                             int             comment '渠道id',
channel_put                                            string          comment '渠道关键词',
from_project_id                                        bigint          comment '来自的楼盘id',
product_id                                             int             comment '项目id',
sub_product_id                                         int             comment '子项目id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_appsite_op'
;
