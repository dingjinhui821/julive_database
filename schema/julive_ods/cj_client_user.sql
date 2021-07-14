drop table if exists ods.cj_client_user;
create external table ods.cj_client_user(
id                                                     bigint          comment '标识,下发客户端时需要md5加密',
unique_id                                              string          comment 'cj_device_info.unique_id',
app_id                                                 int             comment 'app标识，默认为101',
app_version                                            string          comment 'app版本，默认1.0.0',
agency                                                 string          comment '下载渠道(应用市场标识)',
agency_id                                              int             comment '下载渠道id(应用市场标识映射)',
login_id                                               bigint          comment '跟设备绑定的用户id',
access_token                                           string          comment 'access_token',
last_login_ip                                          string          comment '用户ip，ip2long',
last_login_time                                        int             comment '最后登录时间',
sensors_distinct_id                                    string          comment '神策id',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
jpush_regid                                            string          comment '极光推送id',
flag                                                   int             comment '用来判断token过期得依据',
sub_app_id                                             int             comment 'app_id,马甲包+主包',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_client_user'
;
