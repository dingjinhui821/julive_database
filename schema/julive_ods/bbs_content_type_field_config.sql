drop table if exists ods.bbs_content_type_field_config;
create external table ods.bbs_content_type_field_config(
id                                                     int             comment '自增id',
content_type_id                                        int             comment '内容分类配置表id',
field_title                                            string          comment '字段名称',
field_type                                             int             comment '字段类型 (1-单选,2-多选,3-下拉,4-文本,5-文本域,6-区间框,7-时间插件)',
sign                                                   string          comment '唯一标识',
json_data                                              string          comment '题目详细数据',
is_must                                                int             comment '是否必填 1 是 2 否',
is_fixed                                               int             comment '是否固定字段 1 是 2 否',
forum_type                                             int             comment '1:所有 10:bug,20:建议,30:咨询 40:公告 50 消息',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_content_type_field_config'
;
