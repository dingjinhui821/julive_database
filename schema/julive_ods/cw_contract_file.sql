drop table if exists ods.cw_contract_file;
create external table ods.cw_contract_file(
id                                                     int             comment '',
approval_id                                            int             comment '审批id',
contract_approval_id                                   int             comment '合同id',
contract_id                                            string          comment '业务合同id',
contract_name                                          string          comment '合同名称',
partner_id                                             string          comment '合作方id多个逗号分隔',
initiator                                              int             comment '发起人',
is_effect                                              int             comment '是否有效(1-有效，2-无效)',
is_outrange_time                                       int             comment '是否超出规定收回盖章合同日期(1-未超出，2-超出)暂时弃用',
status                                                 int             comment '合同归档类型(1-未上传盖章合同，2-法务未确认，3-合同已归档 4.合同作废 5.作废申请中)',
cooperation_start_time                                 int             comment '合同开始时间',
cooperation_end_time                                   int             comment '合同结束时间',
contract_apply                                         int             comment '合同提交人',
contract_depart                                        int             comment '合同提交人部门',
contract_back_time                                     int             comment '规定收回盖章合同日期',
contract_file_people                                   int             comment '合同归档人',
contract_file_time                                     int             comment '合同归档时间',
nature_num                                             int             comment '合同id自然数字',
cancellation_content                                   string          comment '合同作废理由',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
contract_seal_status                                   int             comment '合同是否盖章 1是 2否',
seal_contract_back_status                              int             comment '盖章合同是否收回 1 是 2 否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_contract_file'
;
