drop table if exists ods.yw_tags;
create external table ods.yw_tags(
id                                                     int             comment '',
title                                                  string          comment '标签名称',
creator                                                int             comment '创建人',
type                                                   int             comment '类型:1系统标签 2用户标签',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
number                                                 string          comment '编号',
show_index                                             int             comment '排序',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_tags'
;
