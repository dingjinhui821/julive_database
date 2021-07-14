drop table if exists ods.cj_house_type_img;
create external table ods.cj_house_type_img(
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_house_type_img'
;
