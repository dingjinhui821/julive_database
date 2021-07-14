drop table if exists ods.yw_search_config;
create external table ods.yw_search_config(
id                                                     bigint          comment '',
city_id                                                bigint          comment '城市id',
site_type                                              string          comment '所属网站',
parent_key                                             string          comment '父级的key',
child_key                                              string          comment '子项的key 如a0',
child_show_text                                        string          comment '子项的值 如:朝阳',
search_sql                                             string          comment '子项的搜索条件',
show_index                                             bigint          comment '排序',
is_show                                                bigint          comment '是否显示 1显示 2不显示',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
table_name                                             string          comment '搜索的表名称 cj_project,cj_project',
opensearch_filter                                      string          comment '在opensearch中的过滤条件',
seo_text                                               string          comment 'seo对应文案',
search_between_value                                   string          comment '搜索范围值，逗号分隔，最大最小值边缘数据用min,max代替',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_search_config'
;
