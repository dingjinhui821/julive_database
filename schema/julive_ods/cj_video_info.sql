drop table if exists ods.cj_video_info;
create external table ods.cj_video_info(
id                                                     int             comment '',
type                                                   int             comment '分类 1信息流 2资讯',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
video_long                                             string          comment '视频时长',
video_header_img                                       string          comment '视频头图',
video_img                                              string          comment '视频头图(截取)',
video_url                                              string          comment '视频地址',
video_suffix                                           string          comment '视频格式',
object_id                                              int             comment '对象id',
play_completed_num                                     int             comment '播放完成次数',
video_width                                            string          comment '视频宽度',
video_height                                           string          comment '视频高度',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_video_info'
;
