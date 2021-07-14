drop table if exists ods.cj_project_tag;
create external table ods.cj_project_tag(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
tag_id                                                 int             comment '标签id',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_tag'
;
