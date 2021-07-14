drop table if exists ods.sys_sensors_black_ip;
create external table ods.sys_sensors_black_ip(
id                                                     int             comment '',
ip                                                     string          comment '非法ip',
create_datetime                                        string          comment '创建时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/sys_sensors_black_ip'
;
