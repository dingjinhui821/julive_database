drop table if exists ods.ex_bank_flow;
create external table ods.ex_bank_flow(
id                                                     int             comment 'id',
bank_type                                              int             comment '银行类型 1.平安 2.招商',
bank_name                                              string          comment '银行名称',
bank_flow_number                                       string          comment '银行流水号',
city_id                                                int             comment '城市id',
payment_company                                        string          comment '付款公司',
payment_account                                        string          comment '付款账号',
cw_accounts_id                                         int             comment '账户id（cw_accounts.id）用作找公司账号',
gathering_company_account                              string          comment '收款公司账号',
cw_company_config_id                                   int             comment '收款公司账号id 用作找公司名',
gathering_company_name                                 string          comment '收款公司名称',
arrival_time                                           int             comment '到账时间',
arrival_money                                          double          comment '到账金额',
refund_money                                           double          comment '退款金额',
net_income                                             double          comment '净收入',
verified_money                                         double          comment '已核销金额',
unverified_money                                       double          comment '待核销金额',
margin_money                                           double          comment '差额',
verify_status                                          int             comment '核销状态（1.待核销 2.已完成 3.部分核销 4.无需审核） 默认1',
verify_time                                            int             comment '核销时间',
payback_type                                           int             comment '回款类型（1自动，2手动） 默认1',
is_added_cash_flow                                     int             comment '作废 是否已加入现金流（1:已加入2:未加入）默认2',
data_status                                            int             comment '数据状态 1正常 2删除 默认1',
flow_unique                                            string          comment '流水唯一标志（md5 32位字符，针对自动拉取）',
receipt_type                                           int             comment '收款类型 10应收 20预收',
updator                                                int             comment '更新人',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
is_finance_add_fee                                     int             comment '谁操作添加现金流 1财务 2未加入过 3渠道',
add_cash_time                                          int             comment '加入现金流时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_bank_flow'
;
