drop table if exists ods.xpt_opensearch_search_config;
create external table ods.xpt_opensearch_search_config(
id                                                     int             comment 'id',
city_id                                                int             comment '城市id',
site_type                                              int             comment '所属网站类型',
parent_id                                              int             comment '父id',
group_key                                              string          comment '分类标识,如:a代表区域',
show_key                                               string          comment 'url中要展示的key',
show_text                                              string          comment '显示的值 如:朝阳',
seo_text                                               string          comment 'seo对应文案',
search_field                                           string          comment '在opensearch中搜索的字段名',
search_value                                           string          comment '搜索的值,如果是范围值用,逗号分隔,最大最小值边缘数据用min,max代替',
search_filter                                          string          comment '在opensearch中的筛选条件',
show_index                                             int             comment '排序',
is_show                                                int             comment '是否显示 1显示 2不显示',
is_fixed                                               int             comment '是否在列表上面固定标签 1固定 2不固定',
updator                                                int             comment '修改者',
creator                                                int             comment '创建者',
update_datetime                                        int             comment '修改时间',
create_datetime                                        int             comment '创建时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_opensearch_search_config'
;
