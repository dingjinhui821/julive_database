drop table if exists ods.yw_deal_reward_history;
create external table ods.yw_deal_reward_history(
id                                                     int             comment 'id',
reward_id                                              int             comment '合同外收入id',
deal_id                                                int             comment '成交单id',
money                                                  double          comment '成交奖金额',
reward_time                                            int             comment '奖励时间',
review                                                 int             comment '财务审核状态，0未审核，1审核成功，2审核失败 3审批通过撤销',
review_reason                                          string          comment '驳回原因',
revoke_reason                                          string          comment '撤销原因',
release_reason                                         string          comment '释放原因',
status                                                 int             comment '状态，1正常 2删除',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
cw_accounts_id                                         int             comment '银行账户:cw_accounts 表 id',
cw_accounting_class_id                                 int             comment 'cw_accounting_class 表 id',
bank_flow_id                                           int             comment 'bank_flow_id 银行流水主表id',
verify_location                                        int             comment '审核位置:1核销2付款列表',
verify_status                                          int             comment '(核销操作变更 每次此字段变更需要根据对应关系修改 review 字段) 核销状态 : 0等待提交 1.等待审核 2审核通过 3.无需审核 4.审核驳回 5.释放金额 6释放审批中 7释放审批驳回 8释放撤回 9成交单退款',
refund_money                                           double          comment '成交奖的退款金额',
audit_datetime                                         int             comment '审核时间',
auditor                                                int             comment '审核人',
verify_batch_id                                        int             comment '核销批次id ex_flow_verify_batch表id',
release_verify_status                                  int             comment '银行回款核销发起释放审批时状态verify_status的值',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_deal_reward_history'
;
