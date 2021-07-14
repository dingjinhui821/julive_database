drop table if exists julive_dim.dim_city;
create external table julive_dim.dim_city(
skey                                                   int             comment '主键',
city_id                                                bigint          comment '源系统城市id',
city_name                                              string          comment '城市名称',
city_seq                                               string          comment '开城顺序',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_dim/dim_city'
;
