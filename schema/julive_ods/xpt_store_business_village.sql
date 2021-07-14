drop table if exists ods.xpt_store_business_village;
create external table ods.xpt_store_business_village(
id                                                     bigint          comment '自增id',
store_id                                               int             comment '门店id',
village_id                                             bigint          comment '小区id',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_store_business_village'
;
