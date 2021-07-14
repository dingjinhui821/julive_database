drop table if exists ods.sa_pad_employee_use_time;
create external table ods.sa_pad_employee_use_time(
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/sa_pad_employee_use_time'
;
