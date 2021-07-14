drop table if exists ods.cj_project_special_label;
create external table ods.cj_project_special_label(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
name                                                   string          comment '标签名称',
project_id_type                                        string          comment '楼盘id与类型拼接的冗余字段',
tag_source                                             int             comment '标签来源，1认购，2带看，3留电',
status                                                 int             comment '状态 1显示 2隐藏',
type                                                   int             comment '类型',
rank                                                   int             comment '优先级(小的排在前面):楼盘余量10>户型余量20>即将开盘30>用户动态40',
value                                                  int             comment '存储数据值，如楼盘余量值',
user_action_datetime                                   int             comment '楼盘-用户动态时间（认购:subscribe_datetime -带看:plan_real_begin_datetime-留电:create_datetime ）',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_special_label'
;
