drop table if exists ods.hr_approval_record;
create external table ods.hr_approval_record(
id                                                     bigint          comment '',
approval_id                                            int             comment '审批id',
approver                                               string          comment '审批人',
approval_status                                        int             comment '审批状态 1正在审批  2 通过 3驳回 4撤销',
approval_note                                          string          comment '审批备注',
audit_time                                             int             comment '审核时间（通过或驳回时间）',
create_datetime                                        int             comment '添加时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '添加人',
updator                                                int             comment '更新人',
post_id                                                string          comment '岗位id 逗号分隔，如果审批人类型是固定岗位或一般人，则有值',
sort                                                   int             comment '排序',
approvaler_type                                        int             comment '审批人类型标识1.一级部分负责人，4.二级部门负责人，7.三级部门负责人，10.四级部门负责人，28固定岗位，29印章执行',
operator                                               int             comment '审批通过/驳回人',
level                                                  int             comment '审批级别',
offjob_time                                            int             comment '最终实际离职日期',
is_attachment_upload                                   int             comment '附件是否已经上传 1 是 2否',
upload_tips                                            string          comment '上传附件提示内容',
is_require_offjobtime                                  int             comment '实际离职日期是否必填1 是 2 否',
is_require_upload                                      int             comment '上传附件是否必须 1 是 2 否',
approval_start_time                                    int             comment '审核开始时间',
approval_end_time                                      int             comment '',
seal_id                                                int             comment '印章id',
department_type                                        int             comment '审批部门类型 1业务部门 2财务部门',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_approval_record'
;
