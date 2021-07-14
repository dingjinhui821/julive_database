drop table if exists ods.yw_ios_utm_source;
create external table ods.yw_ios_utm_source(
id                                                     int             comment '',
user_id                                                bigint          comment '订单的用户id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '记录更新时间',
order_id                                               bigint          comment '订单id',
order_create_datetime                                  int             comment '订单创建时间',
product_id                                             string          comment '',
utm_source                                             string          comment 'utm_source',
select_city                                            int             comment '居理的城市id',
os                                                     string          comment '操作系统',
app_version                                            string          comment '居理app的版本',
city                                                   string          comment '神策的定位的城市',
device_id                                              string          comment 'appinstall事件的device_id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_ios_utm_source'
;
