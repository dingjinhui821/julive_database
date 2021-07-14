drop table if exists ods.cj_project_level;
create external table ods.cj_project_level(
id                                                     int             comment '主键id',
project_id                                             int             comment '楼盘id',
project_level                                          int             comment '楼盘分级',
uv_update_time                                         int             comment 'uv更新时间',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_level'
;
