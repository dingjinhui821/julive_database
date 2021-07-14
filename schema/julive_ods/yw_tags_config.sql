drop table if exists ods.yw_tags_config;
create external table ods.yw_tags_config(
id                                                     bigint          comment '',
tag_model                                              string          comment '标签对应的model',
tag_value                                              string          comment '标签对应的定义常量值逗号分隔',
tag_reverse_value                                      int             comment '合理标签值',
tag_name                                               string          comment '标签名称',
tag_reverse_name                                       string          comment '合理标签名称',
`desc`                                                 string          comment '标签描述',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
delay_time                                             int             comment '计算绩效延迟时间 用于兼容某些标签时间是昨天打今天算 单位秒  正数 往前推n秒  负数 往后推n秒',
tag_type                                               int             comment '标签分类 1.联系类 2.带看类 3.认购类 4.其他类',
is_hidden                                              int             comment '是否隐藏 0.展示 1.隐藏',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_tags_config'
;
