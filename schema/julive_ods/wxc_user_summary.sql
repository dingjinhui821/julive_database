drop table if exists ods.wxc_user_summary;
create external table ods.wxc_user_summary(
id                                                     int             comment '',
appid                                                  string          comment '微信的appid',
server_name                                            string          comment '微信账号的名称',
ref_date                                               string          comment '数据日期',
user_source                                            int             comment '-1:默认值，其他值表示的含义,请看微信',
new_user                                               int             comment '新增的用户数量',
cancel_user                                            int             comment '取消关注的用户数量,new_user减去cancel_user即为净增用户数量',
cumulate_user                                          int             comment '总用户量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '数据更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/wxc_user_summary'
;
