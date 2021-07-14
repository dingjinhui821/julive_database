drop table if exists ods.cj_brand_developer;
create external table ods.cj_brand_developer(
id                                                     int             comment '',
name                                                   string          comment '开发商名称',
long_desc                                              string          comment '开发商描述–长',
short_desc                                             string          comment '开发商描述–短',
app_desc                                               string          comment '开发商描述–app',
status                                                 int             comment '状态（1:正常0:删除）',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_brand_developer'
;
