drop table if exists ods.yw_share_browse_record;
create external table ods.yw_share_browse_record(
id                                                     bigint          comment '自增id',
open_id                                                string          comment '微信的openid',
user_id                                                bigint          comment '用户登录在登录时的user_id',
phone                                                  string          comment '用户登录时的手机号',
create_datetime                                        int             comment '记录创建时间',
share_id                                               int             comment '分享id',
show_type                                              int             comment '分享类型1:单楼盘,2:多楼盘,3:文章类型,4:签约前提醒,5:认购前提醒,6:看房报告,7:带看前行程确认',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '资料包的对应的order_id',
nickname                                               string          comment '微信的昵称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_browse_record'
;
