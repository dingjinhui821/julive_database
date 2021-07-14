drop table if exists ods.yw_active_from_third;
create external table ods.yw_active_from_third(
id                                                     int             comment '自增id',
idfa                                                   string          comment 'idfa',
ip                                                     string          comment 'ip地址',
device                                                 string          comment '设备型号',
os                                                     string          comment 'ios版本',
create_datetime                                        int             comment '记录创建时间',
update_datetime                                        int             comment '更新时间',
sub_app_id                                             int             comment '居理的sub_app_id',
type                                                   int             comment '1:试客',
active_datetime                                        int             comment '激活时间',
callback_url                                           string          comment '回调url',
source_channel                                         int             comment '渠道来源 1傲娇 2巨掌 3优聚 4爱盈利',
is_active                                              int             comment '是否激活 1是 2否',
is_callback                                            int             comment '是否回传给第三方 1是 2否 3回调接口失败',
is_cheat                                               int             comment '是否作弊',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_active_from_third'
;
