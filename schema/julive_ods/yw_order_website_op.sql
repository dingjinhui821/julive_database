drop table if exists ods.yw_order_website_op;
create external table ods.yw_order_website_op(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
op_id                                                  bigint          comment 'oplog的id',
user_id                                                string          comment '用户id串',
user_ip                                                string          comment '用户ip',
create_datetime                                        int             comment '创建时间',
page_id                                                int             comment '来源页面id',
component_id                                           int             comment '组件id',
referer                                                string          comment '',
op_type                                                int             comment '操作类型',
op_detail                                              string          comment '操作详细信息，用json表示',
update_datetime                                        int             comment '更新时间',
source                                                 int             comment '来源:1pc端，2移动端',
channel_id                                             int             comment '渠道id，1百度sem，2百度联盟',
channel_item                                           string          comment '',
channel_put                                            string          comment '',
from_project_id                                        bigint          comment '来自的楼盘id',
last_last_op_type                                      int             comment '上上次的optype',
product_id                                             int             comment '产品端',
media_source                                           string          comment '媒体来源',
c_creative                                             string          comment '创意id',
c_adposition                                           string          comment '展现排名id',
c_keywordid                                            string          comment '关键词id',
c_matchtype                                            string          comment '匹配方式',
c_pagenum                                              string          comment '页码',
c_first_url                                            string          comment '首次访问url',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_website_op'
;
