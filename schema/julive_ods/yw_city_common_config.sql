drop table if exists ods.yw_city_common_config;
create external table ods.yw_city_common_config(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
alloc_switch                                           int             comment '是否开启分配策略:1a组2b组3ab组',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
type                                                   int             comment '类型:1订单分配2北斗计划',
is_replace_report                                      int             comment '是否替换首电报告（0.不替换 1.替换）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_city_common_config'
;
