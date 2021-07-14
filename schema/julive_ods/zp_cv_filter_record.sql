drop table if exists ods.zp_cv_filter_record;
create external table ods.zp_cv_filter_record(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
type                                                   int             comment '操作结果 1 通过 2 未通过',
follow_person_id                                       int             comment '跟进人id',
creator                                                int             comment '创建者id',
updator                                                int             comment '更新者id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
config_json_data                                       string          comment '模板数据',
p_id                                                   int             comment '模板id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_cv_filter_record'
;
