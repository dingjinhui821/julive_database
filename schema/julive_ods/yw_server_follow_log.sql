drop table if exists ods.yw_server_follow_log;
create external table ods.yw_server_follow_log(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
note                                                   string          comment '备注',
updator                                                bigint          comment '修改人id',
job_number                                             string          comment '修改人工号',
updator_name                                           string          comment '修改人姓名',
update_datetime                                        int             comment '修改时间',
create_datetime                                        int             comment '创建时间',
is_distribute                                          int             comment '同 yw_order 表数据',
distribute_category                                    int             comment '同 yw_order 表数据',
maybe_customer                                         int             comment '同 yw_order 表数据',
order_status                                           int             comment '同 yw_order 表数据',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_server_follow_log'
;
