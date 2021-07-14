drop table if exists ods.cj_fields;
create external table ods.cj_fields(
id                                                     int             comment 'id',
field_name                                             string          comment '字段标识',
field_desc                                             string          comment '字段描述',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_fields'
;
