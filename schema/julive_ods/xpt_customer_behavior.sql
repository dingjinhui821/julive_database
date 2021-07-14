drop table if exists ods.xpt_customer_behavior;
create external table ods.xpt_customer_behavior(
id                                                     bigint          comment '自增id',
object                                                 int             comment '来源 1店铺 2房源 3咨询 4问问 5留电口',
operation                                              int             comment '行为 1点击个人名片进入 2海报扫码进入 3微信搜索小程序进入（普通浏览） 4微信公众号-底部爆款楼盘按钮（浏览） 5分享给微信好友 6生成海报分享 7留电口 （点击）',
target_id                                              bigint          comment '目标id',
target_type                                            int             comment '目标类型 1新房 2二手房 3咨询 4问问',
operate_time                                           int             comment '操作时间',
operate_num                                            int             comment '操作次数',
total_visit_num                                        int             comment '访问总数',
from_page                                              string          comment '来源页面',
from_item                                              string          comment '来源模块',
agent_id                                               bigint          comment '经纪人id',
customer_id                                            bigint          comment '客户id',
optype_id                                              int             comment '客户昵称',
system                                                 string          comment '操作系统',
network                                                string          comment '网络环境',
openid                                                 string          comment '微信openid',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_customer_behavior'
;
