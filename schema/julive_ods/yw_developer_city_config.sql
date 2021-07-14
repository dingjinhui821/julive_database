drop table if exists ods.yw_developer_city_config;
create external table ods.yw_developer_city_config(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
city_id                                                int             comment '虚拟城市id',
status                                                 int             comment '状态（1.正常 2.关闭）',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_developer_city_config'
;
