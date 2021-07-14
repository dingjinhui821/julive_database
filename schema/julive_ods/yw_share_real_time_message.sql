drop table if exists ods.yw_share_real_time_message;
create external table ods.yw_share_real_time_message(
id                                                     int             comment 'id',
wechat_name                                            string          comment '微信昵称',
visitor_id                                             string          comment 'open_id',
event_type                                             int             comment '事件类型，1收藏，2取消收藏，3点赞，4取消点赞,5分享，6浏览',
material_id                                            int             comment '资料id',
share_type                                             int             comment '分享类型:1单楼盘2多楼盘3文章',
adviser_id                                             bigint          comment '咨询师id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
is_read                                                int             comment '是否已读0:未读,1:已读',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_real_time_message'
;
