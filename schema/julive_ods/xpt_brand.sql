drop table if exists ods.xpt_brand;
create external table ods.xpt_brand(
id                                                     int             comment '自增id',
city_id                                                int             comment '门店城市',
expand_head                                            bigint          comment '经拓负责人',
brand_name                                             string          comment '品牌商名称',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_brand'
;
