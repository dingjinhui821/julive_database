drop table if exists ods.xpt_brand_head;
create external table ods.xpt_brand_head(
id                                                     int             comment '自增id',
brand_id                                               int             comment '品牌商id',
user_name                                              string          comment '负责人姓名',
mobile                                                 string          comment '负责人手机号',
job                                                    string          comment '职位',
is_delete                                              int             comment '是否删除 1.是， 2.否',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_brand_head'
;
