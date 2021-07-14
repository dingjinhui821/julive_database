drop table if exists ods.cj_project_imgs_history;
create external table ods.cj_project_imgs_history(
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_imgs_history'
;
