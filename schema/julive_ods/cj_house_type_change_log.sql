drop table if exists ods.cj_house_type_change_log;
create external table ods.cj_house_type_change_log(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
house_type_id                                          int             comment '户型id',
house_on_sale_num                                      int             comment '户型在售套数',
total_count                                            int             comment '户型总套数',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_house_type_change_log'
;
