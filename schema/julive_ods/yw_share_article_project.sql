drop table if exists ods.yw_share_article_project;
create external table ods.yw_share_article_project(
id                                                     int             comment 'id',
project_id                                             int             comment '楼盘id',
share_id                                               int             comment '分享id',
article_id                                             int             comment '文章id',
employee_id                                            bigint          comment '咨询师id',
project_desc                                           string          comment '推荐理由',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_article_project'
;
