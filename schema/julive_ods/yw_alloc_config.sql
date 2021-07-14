drop table if exists ods.yw_alloc_config;
create external table ods.yw_alloc_config(
id                                                     int             comment '',
customer_count                                         int             comment '自定义总量',
skip_info                                              string          comment '步长配置',
max_loop                                               int             comment '最大循环次数',
status                                                 int             comment '状态1显示2隐藏',
retain_order_num                                       int             comment '保留订单数',
city_id                                                int             comment '城市id',
rank_day                                               int             comment '计算排名查询范围',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_alloc_config'
;
