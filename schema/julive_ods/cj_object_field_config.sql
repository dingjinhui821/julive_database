drop table if exists ods.cj_object_field_config;
create external table ods.cj_object_field_config(
id                                                     int             comment '',
city_id                                                int             comment '城市编号',
cate                                                   int             comment '分类1业务2系统',
group_name                                             string          comment '组名（类似于字段名）',
show_group_name                                        string          comment '页面显示',
name                                                   string          comment '常量名(类似于字段值的名称)',
value                                                  string          comment '常量值',
show_txt                                               string          comment '页面显示',
is_show                                                int             comment '是否显示1显示2否',
show_index                                             int             comment '相同字段不同选项的显示顺序',
scene                                                  string          comment '场景',
`desc`                                                 string          comment '描述',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_object_field_config'
;
