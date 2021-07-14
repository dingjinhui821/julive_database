drop table if exists ods.live_room_user;
create external table ods.live_room_user(
id                                                     int             comment 'id',
room_id                                                int             comment '房间id',
source                                                 int             comment '用户来源 1-用户 2-支撑系统',
user_id                                                int             comment '用户id',
flag                                                   int             comment '1-视频发起人 2-视频参与人',
join_time                                              int             comment '加入时间',
leave_time                                             int             comment '离开时间',
status                                                 int             comment '状态 1-在线 2-离线 3-呼叫中 4-未接听 5-已拒绝',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/live_room_user'
;
