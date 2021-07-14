drop table if exists ods.cj_sms_log;
create external table ods.cj_sms_log(
id                                                     int             comment '',
phone                                                  string          comment '短信手机号',
content                                                string          comment '短信内容',
prov_smsid                                             string          comment '短信提供方的消息id',
status                                                 int             comment '发送短信结果状态，1成功，0失败',
provider                                               string          comment '短信提供商，zhuoyun卓云，ucpaas云之讯',
create_datetime                                        bigint          comment '创建时间',
prov_time                                              string          comment '短息提供商记录的短信时间',
return_str                                             string          comment '发短信接口返回的字符串',
tpl                                                    string          comment '短信模板关键字',
ip                                                     string          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_sms_log'
;
