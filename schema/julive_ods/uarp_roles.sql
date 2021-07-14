drop table if exists julive_ods.uarp_roles;
create external table julive_ods.uarp_roles(
id                                                     int             comment '自增id',
track_id                                               int             comment '埋点id',
name                                                   string          comment '字段的中文名',
value                                                  string          comment '字段名',
type                                                   string          comment '扩展字段，非扩展字段
是否是扩展字段，扩展字段需要is_not_null来判断是否能不穿。。非扩展字段根据是否是空来判断要不要传值。',
extend_type                                            string          comment '默认是string类型，',
comments                                               string          comment '字段备注',
range_values                                           string          comment '范围取值类型的起始值',
is_not_null                                            int             comment '是否必传。0是可以为空，1是不能为空',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
is_delete                                              int             comment '是否被逻辑删除
0:未删除，1:删除',
creator                                                int             comment '创建者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/uarp_roles'
;
