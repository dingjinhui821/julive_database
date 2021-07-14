drop table if exists ods.bd_building_relation;
create external table ods.bd_building_relation(
id                                                     bigint          comment '',
project_id                                             bigint          comment '楼盘id',
building_id                                            bigint          comment '楼栋id',
business_type                                          int             comment '类型 1证件楼栋 2户型',
business_id                                            bigint          comment '业务id',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_building_relation'
;
