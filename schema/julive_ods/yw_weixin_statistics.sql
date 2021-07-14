drop table if exists ods.yw_weixin_statistics;
create external table ods.yw_weixin_statistics(
id                                                     bigint          comment 'id',
visitor_id                                             string          comment 'open_id',
order_id                                               bigint          comment '订单id',
material_id                                            int             comment '资料id',
share_type                                             int             comment '分享类型 1单楼盘2多楼盘3文章',
views                                                  int             comment '累计浏览次数',
likes                                                  int             comment '累计点赞',
shares                                                 int             comment '累计分享',
collections                                            int             comment '累计收藏',
view_time                                              int             comment '浏览时长',
module                                                 int             comment '重点关注模块',
create_datetime                                        int             comment '创建事件',
update_datetime                                        int             comment '更新事件',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
reward                                                 int             comment '打赏金额',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_weixin_statistics'
;
