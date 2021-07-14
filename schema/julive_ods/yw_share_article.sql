drop table if exists ods.yw_share_article;
create external table ods.yw_share_article(
id                                                     int             comment 'id',
article_absorbed_id                                    bigint          comment 'yw_article_absorbed表id',
category_id                                            int             comment '分类id 10:政策 20:区域 30:居理',
order_id                                               bigint          comment '订单id',
share_datetime                                         int             comment '分享时间',
share_people                                           bigint          comment '分享人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
wechatcode_url                                         string          comment '生成的二维码地址',
city_id                                                bigint          comment '城市id',
is_show                                                int             comment '资料包是否展示 1展示 0不展示',
share_name                                             string          comment '资料包名称',
share_from                                             int             comment '分享来源1backend 2app',
share_mode                                             int             comment '分享来源1微信分享2短信分享',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_article'
;
