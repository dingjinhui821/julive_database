drop table if exists ods.cj_fields_result;
create external table ods.cj_fields_result(
id                                                     bigint          comment 'id',
business_id                                            int             comment '业务id',
business_type                                          int             comment '业务类型 1.楼盘， 2.户型， 3.楼栋， 4.许可证， 5.大数据动态',
source                                                 int             comment '源 100大数据',
field_id                                               int             comment '字段id',
field_value                                            string          comment '字段值',
judge_result                                           int             comment '判错结果 1.对， 2.错',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_fields_result'
;
