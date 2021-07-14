drop table if exists ods.hr_post_cate;
create external table ods.hr_post_cate(
id                                                     int             comment '岗位分类id',
name                                                   string          comment '岗位分类名称',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_post_cate'
;
