drop table if exists ods.hr_post_history;
create external table ods.hr_post_history(
id                                                     int             comment '岗位id',
cate_id                                                int             comment '分类id',
post_id                                                int             comment '岗位id',
name                                                   string          comment '岗位名称',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_post_history'
;
