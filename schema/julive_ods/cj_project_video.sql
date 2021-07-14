drop table if exists ods.cj_project_video;
create external table ods.cj_project_video(
id                                                     bigint          comment 'id',
project_id                                             int             comment '楼盘id',
city_id                                                int             comment '城市id',
video_path                                             string          comment '视频地址',
title                                                  string          comment '视频标题',
status                                                 int             comment '是否显示 1显示 2隐藏',
description                                            string          comment '正文描述',
is_delete                                              int             comment '是否删除 1是 2否',
video_img_path                                         string          comment '视频头图地址',
video_img_info                                         string          comment '视频头图宽高信息',
create_datetime                                        int             comment '更新时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
type                                                   int             comment '类型 1楼盘视频',
video_info                                             string          comment '视频宽高信息',
video_duration                                         int             comment '视频时长（秒）',
video_point                                            string          comment '视频打点信息',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_video'
;
