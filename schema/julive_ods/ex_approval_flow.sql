drop table if exists ods.ex_approval_flow;
create external table ods.ex_approval_flow(
id                                                     int             comment '',
ex_approval_id                                         int             comment '审批id',
approver                                               string          comment '审批人',
operator                                               int             comment '审批通过/驳回人',
status                                                 int             comment '审批状态 1正在审批  2 通过 3驳回 4撤销 5等待审批',
approval_note                                          string          comment '审批备注',
approvaler_type                                        int             comment '审批人类型标识 1直属拓展主管 2直属渠道经理 3直属城市经理  5固定人',
level                                                  int             comment '审批级别',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
operate_datetime                                       int             comment '操作时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_approval_flow'
;
