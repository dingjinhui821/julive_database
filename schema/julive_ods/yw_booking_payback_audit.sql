drop table if exists ods.yw_booking_payback_audit;
create external table ods.yw_booking_payback_audit(
id                                                     bigint          comment '主键id',
audit_group_id                                         string          comment '审核组id',
booking_id                                             bigint          comment '订房id',
audit_role                                             int             comment '审核角色 1:主管 2:财务出纳 3:商务',
audit_datetime                                         int             comment '审核时间',
auditor                                                int             comment '审核人',
audit_status                                           int             comment '审核状态 1:审核通过 2:审核驳回',
cause                                                  string          comment '驳回理由',
submit_datetime                                        int             comment '发起审核时间',
submit_cause                                           int             comment '发起退款原因',
apply_type                                             int             comment '退款申请类型 1:用户 2:系统 3:转认购',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_booking_payback_audit'
;
