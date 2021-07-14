drop table if exists ods.cw_deal_refund_to_bank_flow;
create external table ods.cw_deal_refund_to_bank_flow(
id                                                     int             comment 'id',
bank_flow_id                                           int             comment '银行流水id',
deal_id                                                int             comment '成交单id',
approval_id                                            int             comment '审批id',
refund_to_bank_flow_amount                             double          comment '退到流水中的金额',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
refund_source                                          int             comment '退款来源1.实收2成交奖',
refund_source_id                                       int             comment '退款来源关联id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_deal_refund_to_bank_flow'
;
