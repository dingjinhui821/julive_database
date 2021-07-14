drop table if exists ods.yw_article_absorbed;
create external table ods.yw_article_absorbed(
id                                                     int             comment '',
city_id                                                bigint          comment '城市id',
category_id                                            int             comment '分类id 10:政策 20:区域 30:居理',
`group`                                                int             comment '头条分组:1.焦点、2.推房、3.百科、4.品牌、5.个人',
content_type                                           int             comment '内容类型 0.正常 1.h5 2.非h5',
come_from                                              string          comment '文章来源',
district_id                                            int             comment '区域id',
title                                                  string          comment '标题',
content                                                string          comment '内容',
status                                                 int             comment '状态:1:展示  2:不展示',
thumb                                                  string          comment '文章缩略图',
share_count                                            int             comment '转发次数',
video_name                                             string          comment '视频名称',
video_pic                                              string          comment '视频头图',
video_url                                              string          comment '视频地址',
sort                                                   int             comment '排序 越大越靠前',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_article_absorbed'
;
