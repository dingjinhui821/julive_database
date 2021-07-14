drop table if exists ods.cj_tag;
create external table ods.cj_tag(
id                                                     int             comment '',
name                                                   string          comment '便签名称',
type                                                   int             comment '类型 1销售状态 2建筑形式或业态 3社区特色 4户型 5自定义',
status                                                 int             comment '状态 1显示 2隐藏',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_tag'
;
