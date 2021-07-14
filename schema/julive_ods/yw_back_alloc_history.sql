drop table if exists ods.yw_back_alloc_history;
create external table ods.yw_back_alloc_history(
id                                                     int             comment '',
order_id                                               int             comment '订单id',
reason_type                                            int             comment '退分配原因，1中介市调 2不符合上户标准 3城市分配错误 4非业务来电 5其他',
remark                                                 string          comment '备注',
is_deduct_service                                      int             comment '是否扣除客服质检，0否，1是',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_back_alloc_history'
;
