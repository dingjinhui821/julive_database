drop table if exists ods.yw_share_project_many;
create external table ods.yw_share_project_many(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
commute_lng                                            string          comment '通勤地址经度',
commute_lat                                            string          comment '通勤纬度',
commute_address                                        string          comment '通勤地址',
share_datetime                                         int             comment '分享时间',
share_people                                           int             comment '分享人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
wechatcode_url                                         string          comment '生成的二维码地址',
city_id                                                bigint          comment '城市id',
share_type                                             int             comment '1单楼盘分享 2多楼盘分享',
is_show                                                int             comment '资料包是否展示 1展示 0不展示',
answer_ids                                             string          comment '问答id，,分隔',
share_name                                             string          comment '资料包名称',
share_mode                                             int             comment '分享方式（1.微信分享 2.短信分享）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_project_many'
;
