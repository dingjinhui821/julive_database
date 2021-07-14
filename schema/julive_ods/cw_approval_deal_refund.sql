drop table if exists ods.cw_approval_deal_refund;
create external table ods.cw_approval_deal_refund(
id                                                     int             comment '主键',
approval_id                                            int             comment '审批id',
deal_id                                                int             comment '成交单id',
original_refund_money                                  double          comment '原始退款金额',
origin_money                                           double          comment '审批单实收金额',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
paid_money                                             double          comment '已付金额',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_approval_deal_refund'
;
