drop table if exists ods.ex_flow_verify_batch;
create external table ods.ex_flow_verify_batch(
id                                                     int             comment 'id',
bank_flow_id                                           int             comment '流水id',
updator                                                int             comment '更新人',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
reason                                                 string          comment '原因',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_flow_verify_batch'
;
