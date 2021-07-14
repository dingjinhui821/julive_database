drop table if exists ods.ex_sign_project;
create external table ods.ex_sign_project(
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_sign_project'
;
