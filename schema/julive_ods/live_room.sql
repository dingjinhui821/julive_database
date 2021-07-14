drop table if exists ods.live_room;
create external table ods.live_room(
id                                                     int             comment '房间id',
source                                                 int             comment '1 用户 2 支撑系统',
user_id                                                int             comment '发起人id',
video_url                                              string          comment '视频地址',
video_time                                             int             comment '视频时长',
user_nums                                              int             comment '参与人数',
create_datetime                                        int             comment '创建时间',
room_status                                            int             comment '0有效 1 已解散',
update_datetime                                        int             comment '更新时间',
mix_stream_start                                       int             comment '混流开始时间',
mix_stream_response                                    string          comment '混流最新响应结果',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/live_room'
;
