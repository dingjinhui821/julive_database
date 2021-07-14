drop table if exists ods.yw_ocpc;
create external table ods.yw_ocpc(
id                                                     int             comment '自增id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
note                                                   string          comment '备注',
active_callback_status                                 int             comment '活状态是否已经回传百度1:已经回传,2:未回传',
os                                                     int             comment 'ios:1,安卓:2',
ip                                                     string          comment '考虑ipv6的情况',
ua                                                     string          comment '数据上报终端设备user agent',
ts                                                     int             comment '时间戳',
pid                                                    string          comment '计划id',
uid                                                    string          comment '单元id',
aid                                                    string          comment '创意id',
click_id                                               string          comment '点击唯一id',
callback_url                                           string          comment 'callback_url接口',
sign                                                   string          comment '签名',
imei_md5                                               string          comment '安卓设备标识，标准32位md5编码，当无法获取imei时传空',
idfa                                                   string          comment 'ios设备标识:原值',
unique_id                                              string          comment '居理生成的unique_id',
app_id                                                 int             comment '居理的app_id',
account_id                                             string          comment '账号的id',
utm_source                                             string          comment '广告渠道',
media_type                                             int             comment '1:百度',
order_id                                               bigint          comment '订单id',
callback_datetime                                      int             comment '回传时间',
callback_date                                          int             comment '回传日期',
comjia_customer_id                                     string          comment '客端生成的唯一标识',
order_callback_status                                  int             comment '订单上户是否已经回传百度1:已经回传,2:未回传',
user_id                                                bigint          comment '居理的用户id',
call_back_type                                         int             comment '1:自然的激活，2:根据回传率生成的激活',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_ocpc'
;
