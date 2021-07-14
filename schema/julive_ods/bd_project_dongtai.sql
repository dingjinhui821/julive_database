drop table if exists ods.bd_project_dongtai;
create external table ods.bd_project_dongtai(
id                                                     int             comment '',
content                                                string          comment '动态',
title                                                  string          comment '标题',
project_id                                             bigint          comment '抓取过来的楼盘id',
source                                                 string          comment '来源',
updator                                                int             comment '修改者',
author                                                 int             comment '审核者',
review                                                 int             comment '0,未审核，1，通过，2，不通过',
update_datetime                                        int             comment '审核时间',
create_datetime                                        int             comment '创建时间',
publish_datetime                                       int             comment '发布时间',
creator                                                bigint          comment '创建者',
source_dongtai_id                                      bigint          comment '源网站的动态id',
parent_dongtai_id                                      int             comment '父动态id',
is_send                                                int             comment '是否发送给大数据 1已发送 2未发送',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_dongtai'
;
