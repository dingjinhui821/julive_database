drop table if exists ods.yw_share_collect;
create external table ods.yw_share_collect(
id                                                     int             comment '自增id',
open_id                                                string          comment '收藏者的open_id',
order_id                                               bigint          comment '订单id',
share_id                                               int             comment '收藏的资料id',
show_type                                              int             comment '分享类型1:单楼盘,2:多楼盘,3:文章类型,4:签约前提醒,5:认购前提醒,6:看房报告,7:带看前行程确认',
create_datetime                                        int             comment '记录创建时间',
update_datetime                                        int             comment '更新时间',
user_id                                                bigint          comment '登录的user_id',
phone                                                  string          comment '手机号',
action_type                                            int             comment '1收藏 2点赞',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_collect'
;
