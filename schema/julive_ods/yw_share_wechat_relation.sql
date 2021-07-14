drop table if exists ods.yw_share_wechat_relation;
create external table ods.yw_share_wechat_relation(
id                                                     bigint          comment '',
wechat_number                                          string          comment '微信号',
share_id                                               int             comment '分享表id',
order_id                                               bigint          comment '订单id',
share_people                                           bigint          comment '分享人',
share_datetime                                         int             comment '分享时间',
type                                                   int             comment '分享类型:1单楼盘2多楼盘3文章',
user_type                                              int             comment '1好友2微信群',
group_id                                               string          comment '微信群id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
city_id                                                int             comment '城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_wechat_relation'
;
