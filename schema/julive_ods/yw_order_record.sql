drop table if exists ods.yw_order_record;
create external table ods.yw_order_record(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
more_mobile                                            string          comment '备用手机号',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_record'
;
