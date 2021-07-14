drop table if exists ods.wxc_user;
create external table ods.wxc_user(
id                                                     int             comment '',
openid                                                 string          comment '',
uniq_id                                                string          comment '唯一的id,那微信号+openid,然后md5的结果',
unionid                                                string          comment '绑定开发者账号才有',
subscribe                                              int             comment '用户是否订阅该公众号标识0:未关注|1:关注',
nickname                                               string          comment '用户的昵称',
sex                                                    int             comment '用户的性别，值为1时是男性，值为2时是女性，值为0时是未知',
wechat_city                                            string          comment '用户所在城市',
country                                                string          comment '用户所在国家',
wechat_province                                        string          comment '用户所在省份',
language                                               string          comment '用户的语言，简体中文为zh_cn',
headimgurl                                             string          comment '用户头像',
subscribe_time                                         int             comment '用户关注时间，为时间戳',
remark                                                 string          comment '公众号运营者对粉丝的备注',
groupid                                                string          comment '用户所在的分组id',
subscribe_scene                                        string          comment '返回用户关注的渠道来源,详情见常量',
appid                                                  string          comment '',
scene_value                                            string          comment '场景值id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
user_id                                                bigint          comment '居里用户的id',
session_key                                            string          comment 'session key',
flag                                                   int             comment '用来检查jwt的有效性',
first_location                                         string          comment '第一次上报的位置(latitude,longitude)',
last_location                                          string          comment '最近一次上报的位置(latitude,longitude)',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/wxc_user'
;
