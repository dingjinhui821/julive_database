drop table if exists ods.wxc_article_total;
create external table ods.wxc_article_total(
id                                                     int             comment '',
appid                                                  string          comment '微信的appid',
server_name                                            string          comment '微信账号的名称',
ref_date                                               string          comment '发布日期日期',
stat_date                                              string          comment '数据日期',
title                                                  string          comment '标题',
msgid                                                  string          comment '消息id_文章的顺序',
int_page_read_user                                     int             comment '图文页（点击群发图文卡片进入的页面）的阅读人数',
int_page_read_count                                    int             comment '图文页的阅读次数',
ori_page_read_user                                     int             comment '原文页的阅读人数',
ori_page_read_count                                    int             comment '原文页的阅读次数',
int_page_from_session_read_count                       int             comment '公众号会话阅读次数',
int_page_from_session_read_user                        int             comment '公众号会话阅读人数',
int_page_from_feed_read_user                           int             comment '朋友圈阅读人数',
int_page_from_feed_read_count                          int             comment '朋友圈阅读次数',
share_count                                            int             comment '分享的次数',
share_user                                             int             comment '分享的人数',
add_to_fav_count                                       int             comment '收藏的次数',
add_to_fav_user                                        int             comment '收藏的人数',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '数据更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/wxc_article_total'
;
