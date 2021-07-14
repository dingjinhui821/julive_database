drop table if exists ods.yw_insure_city;
create external table ods.yw_insure_city(
id                                                     int             comment '',
code                                                   int             comment '城市编码',
name_cn                                                string          comment '城市名称',
parent_code                                            int             comment '上级城市的code',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_insure_city'
;
