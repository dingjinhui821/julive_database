drop table if exists ods.cj_information_flow;
create external table ods.cj_information_flow(
id                                                     int             comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
fictitious_user_id                                     bigint          comment '虚拟用户id',
title                                                  string          comment '标题',
content                                                string          comment '正文',
images_list                                            string          comment '图集[json格式]',
article_id                                             string          comment '文章id 已,分割',
project_id                                             string          comment '楼盘id 已,分割',
status                                                 int             comment '状态 1有效 2无效',
brows_bumber                                           bigint          comment '浏览量',
favor_number                                           bigint          comment '点赞量',
comment_number                                         bigint          comment '评论量',
city_id                                                bigint          comment '城市id',
video_info_id                                          bigint          comment '视频信息表id',
url                                                    string          comment 'url',
apply                                                  string          comment '适用端 按,分割',
tag                                                    string          comment '标签',
case_ids                                               string          comment '案例分析id集(用英文逗号分隔多个)',
publish_datetime                                       int             comment '发布时间',
url_title                                              string          comment 'url 文章对应的标题',
url_image                                              string          comment 'url文章对应的缩略图',
hot_num                                                int             comment '热度值（排序）',
sort                                                   int             comment '排序值 当前表的hot_sum 和 video_info 表 的 play_completed_num 相加结果',
like_num                                               int             comment '点赞的人总数',
is_top                                                 int             comment '是否置顶 1 是 2 否',
top_desc                                               string          comment '置顶文案',
publish_type                                           int             comment '发布类型（1:直接发布，2:定时发布）',
data_type                                              int             comment '数据类型:0.未选 1.图片 2.视频 3.投票',
exposure_num                                           int             comment '卡片曝光量',
is_show_app                                            int             comment '是否在app展示 1展示 2不展示 默认不展示',
video_type                                             int             comment '1 信息流视频  2楼盘视频',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_information_flow'
;
