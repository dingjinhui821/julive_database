drop table if exists ods.yw_weixin_track;
create external table ods.yw_weixin_track(
id                                                     bigint          comment 'id',
track_id                                               bigint          comment '埋点id(大数据匹配)',
visitor_id                                             string          comment 'open_id',
order_id                                               bigint          comment '订单id',
material_id                                            int             comment '资料id',
julive_id                                              int             comment 'julive_id(cj_user id)',
adviser_id                                             bigint          comment '咨询师id',
event_name                                             string          comment '事件名',
event_type                                             int             comment '事件类型，1收藏，2取消收藏，3点赞，4取消点赞,5分享，6浏览',
frompage                                               string          comment 'frompage',
share_type                                             int             comment '分享类型 1单楼盘2多楼盘3文章',
frommodule                                             string          comment 'frommodule',
fromitem                                               string          comment 'fromitem',
fromitemindex                                          string          comment 'fromitemindex',
topage                                                 string          comment 'topage',
tomodule                                               string          comment 'tomodule',
view_time                                              int             comment '页面浏览时长,秒',
aritcle_type                                           int             comment '文章类型 1市场,2区域,3居理',
project_id                                             int             comment '楼盘id',
project_ids                                            string          comment '楼盘id(json数组)',
play_time                                              int             comment '播放时长',
house_type_id                                          int             comment '户型id',
like_action                                            int             comment '点赞动作:1 点赞, 2 取消点赞',
collection_action                                      int             comment '收藏状态:1收藏2取消收藏',
to_url                                                 string          comment 'to_url',
login_state                                            int             comment '登录状态',
district_id                                            int             comment '区域id',
city_id                                                int             comment '城市id',
plate_id                                               int             comment '板块id',
create_datetime                                        int             comment '创建事件',
update_datetime                                        int             comment '更新事件',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
time                                                   bigint          comment '日志行为发生时间',
tab_id                                                 int             comment '标签id, 1=全部2=我的收藏',
is_adviser_phone                                       int             comment '是否是咨询师电话,1,2 1=是2=不是',
module                                                 int             comment '自定义字段，所属模块',
reward                                                 int             comment '打赏金额',
trade_no                                               string          comment '商户订单号',
is_show                                                int             comment '是否展示1展示 0不展示',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_weixin_track'
;
