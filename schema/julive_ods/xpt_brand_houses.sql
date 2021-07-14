drop table if exists ods.xpt_brand_houses;
create external table ods.xpt_brand_houses(
id                                                     int             comment '主键id',
brand_id                                               int             comment '品牌商id',
houses_id                                              int             comment '楼盘id',
houses_name                                            string          comment '楼盘名称',
is_delete                                              int             comment '是否删除 1.是， 2.否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_brand_houses'
;
