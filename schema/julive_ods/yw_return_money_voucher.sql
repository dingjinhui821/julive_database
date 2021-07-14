drop table if exists ods.yw_return_money_voucher;
create external table ods.yw_return_money_voucher(
id                                                     bigint          comment '',
deal_id                                                bigint          comment '成交单id',
order_id                                               bigint          comment '订单id',
follow_id                                              bigint          comment '回款跟进id',
audit_time                                             int             comment '审核时间',
auditor                                                bigint          comment '审核人',
audit_type                                             int             comment '审核结果:1审核中2已通过3未通过',
is_upload                                              int             comment '是否上传:1是2否',
reject_cause                                           string          comment '驳回原因',
reject_channel_cause                                   string          comment '渠道驳回原因',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
audit_employee_type                                    int             comment '咨询师凭证审核状态 1待审核 2审核通过 3审核驳回 4待上传',
audit_user_type                                        int             comment '用户凭证审核状态 1待审核 2审核通过 3审核驳回',
reject_type                                            int             comment '驳回理由:1提交资料与所需要资料类型不匹配2提交资料与对应房号不符3提交资料不清晰无法辨认4提交资料无法证实已经放贷5其他原因',
reject_user_cause                                      string          comment '用户凭证驳回原因',
audit_expand_type                                      int             comment '渠道凭证审核状态 1待审核 2审核通过 3审核驳回 4待上传',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_return_money_voucher'
;
