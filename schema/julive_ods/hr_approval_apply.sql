drop table if exists ods.hr_approval_apply;
create external table ods.hr_approval_apply(
id                                                     bigint          comment '',
type                                                   int             comment '审批类型: 1试用期转正,4见习期转正,7转组,10晋升,13调薪,16离职交接,19考勤申诉,22增加编制,25转岗,28休假,31加班,32出差,33外出,34离职申请,35人力自定义 51合同审批 52非合同类材料用印审批 53交通费审批 54差旅费报销审批 55付款审批 56招待费审批 57各城市印章执行人',
approval_examiner                                      string          comment '被审批人',
current_approver                                       string          comment '当前审批人',
initiator                                              int             comment '发起人',
copy_to                                                string          comment '抄送',
approval_datetime                                      int             comment '审批发起时间',
approval_status                                        int             comment '审批状态 1正在审批  2通过 3审批驳回 4撤销',
approval_note                                          string          comment '审批备注',
revoke_note                                            string          comment '撤销备注',
approval_content                                       string          comment '审批内容',
create_datetime                                        int             comment '添加时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
pay_time                                               int             comment '财务付款时间',
pay_status                                             int             comment '财务付款状态（0:财务未处理、1:已付款、2:未付款,3部分付款）',
initiate_copyer                                        string          comment '发起审批抄送人，逗号分隔（审批发起时勾选的抄送人）',
approval_type_id                                       int             comment '审批类型id',
approval_name                                          string          comment '审批名字',
initiator_city_id                                      int             comment '发起人所在城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_approval_apply'
;
