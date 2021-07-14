drop table if exists ods.cj_device_info;
create external table ods.cj_device_info(
unique_id                                              string          comment '设备唯一标识，md5(mac+imei+sk)生成',
type                                                   int             comment '平台类型，1.android, 2.ios',
agent                                                  string          comment '设备信息(设备品牌型号#分辨率#内核版本)',
version                                                string          comment '内核版本',
imei                                                   string          comment '手机imei',
device_id                                              string          comment '手机标识，android为android_id，苹果为idfa',
wlan_mac                                               string          comment '手机标识，android为wlan mac address，苹果为空',
create_datetime                                        int             comment '创建时间',
idfa                                                   string          comment '',
idfv                                                   string          comment '',
android_id                                             string          comment '',
comjia_customer_id                                     string          comment '客端生成的设备唯一',
md5_imei                                               string          comment 'imei的md5值',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_device_info'
;
