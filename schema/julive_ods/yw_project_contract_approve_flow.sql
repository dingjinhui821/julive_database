drop table if exists ods.yw_project_contract_approve_flow;
create external table ods.yw_project_contract_approve_flow(
id                                                     int             comment '主键id',
yw_project_contract_template_id                        int             comment '楼盘订房合同模板id yw_project_contract_template 表id',
approver                                               int             comment '审批人',
approve_level                                          int             comment '审批级别 同一级别有多人',
approve_status                                         int             comment '0等待审批 1审批中、2审批通过、3审批驳回',
approve_time                                           int             comment '审批时间',
reject_cause                                           string          comment '驳回原因',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
opeator                                                int             comment '操作人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_contract_approve_flow'
;
