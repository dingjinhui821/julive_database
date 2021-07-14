drop table if exists ods.bbs_user_reply;
create external table ods.bbs_user_reply(
id                                                     bigint          comment '',
forum_id                                               bigint          comment '主表id',
employee_id                                            bigint          comment '评论用户id',
content                                                string          comment '评论内容',
reply_datetime                                         int             comment '评论时间',
reply_to                                               bigint          comment '回复评论id',
status                                                 int             comment '状态0正常 1删除',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
is_reply_notice                                        int             comment '回帖是否站内信通知楼主0否 1是',
is_delete_notice                                       int             comment '删除是否站内信通知楼主0否 1是',
is_anonymous                                           int             comment '是否匿名 1 是 2 否',
inviter_id                                             string          comment '邀请人id 多个逗号分隔',
is_floor_anonymous                                     int             comment '回复的楼层是否匿名 1是 2否',
user_replay_info                                       string          comment '楼层回复上一级用户信息',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_user_reply'
;
