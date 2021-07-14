drop table if exists ods.yw_project_contract_template;
create external table ods.yw_project_contract_template(
id                                                     int             comment '主键id',
project_id                                             int             comment '楼盘id',
city_id                                                int             comment '冗余楼盘城市id',
contract_type                                          int             comment '订房合同业务类型 1居理自营合同，2 开发商直营合同',
contract_begin_time                                    int             comment '合同开始时间 0点0分0秒',
contract_end_time                                      int             comment '合同结束时间  23:59:59',
cancellation_time                                      int             comment '审批作废时间',
hr_approve_url                                         string          comment '人力系统审批url json 逗号 分隔',
intention_money                                        double          comment '意向金金额 2位小数',
lock_house_type                                        int             comment '1 付款后x天内锁房 2 截止固定时间锁房',
lock_house_day                                         int             comment '锁房时间 付款后x天内锁房',
lock_house_fixed_date                                  int             comment '截止固定时间锁房',
repeal_employee_id                                     int             comment '审批撤销人',
repeal_cause                                           string          comment '审批撤销原因',
repeal_time                                            int             comment '审批撤销时间',
cancellation_employee_id                               int             comment '审批作废操作人',
cancellation_cause                                     string          comment '作废原因',
cw_supplier_id                                         int             comment '卖方合同主体 id (人力供应商 cw_supplier表id)',
remark                                                 string          comment '备注',
approve_status                                         int             comment '审批状态 0审批中,1审批通过,2审批驳回,3审批撤销,4审批作废',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
contract_template_id                                   int             comment '订房合同模板',
notice_id                                              int             comment '通知卖方谁盖章 居理自营合同:法务角色,开发商直营合同:取开发商人员表id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_contract_template'
;
