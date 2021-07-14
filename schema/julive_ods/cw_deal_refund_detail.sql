drop table if exists ods.cw_deal_refund_detail;
create external table ods.cw_deal_refund_detail(
id                                                     int             comment '主键',
approval_id                                            int             comment '审批id',
deal_id                                                int             comment '成交单id',
fee_detail_id                                          int             comment '明线id',
refund_amount                                          double          comment '退款金额',
refund_datetime                                        int             comment '退款时间',
is_delete                                              int             comment '是否删除 1 是  2 否',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
refund_process_is_done                                 int             comment '退款流程是否完成1完成2未完成',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_deal_refund_detail'
;
