drop table if exists ods.yw_sign_audit_record;
create external table ods.yw_sign_audit_record(
id                                                     bigint          comment '',
group_id                                               string          comment '组id:订单id+签约id+提交审核时间',
order_id                                               bigint          comment '订单id',
sign_id                                                bigint          comment '签约id',
audit_type                                             int             comment '审核类型:1网签审核，2退网签审核，3草签审核，4退草签审核',
submit_audit_datetime                                  int             comment '提交审核时间',
audit_datetime                                         int             comment '审核时间',
auditor                                                bigint          comment '审核人',
audit_role                                             string          comment '审核的角色',
audit_status                                           int             comment '审核状态:0未审核，1审核成功，2审核失败（拓展审核，成交有效性:0未审核，1有效，2无效 3待定）',
reason                                                 string          comment '审核失败原因',
wait_reason                                            string          comment '待定原因',
stop_type                                              int             comment '审核终止原因:1退草签，2退网签，3删除（其它角色审核驳回导致另外角色不再审核是正常流程，不算审核终止）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sign_audit_record'
;
