drop table if exists ods.xpt_store_business_circle;
create external table ods.xpt_store_business_circle(
id                                                     int             comment 'id',
store_id                                               int             comment '门店id',
circle_id                                              int             comment '商圈id',
type                                                   int             comment '类型 1二手房 2新房',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_store_business_circle'
;
