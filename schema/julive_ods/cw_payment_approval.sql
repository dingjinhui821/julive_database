drop table if exists ods.cw_payment_approval;
create external table ods.cw_payment_approval(
id                                                     int             comment 'id',
approval_id                                            int             comment '审批id',
attach_num                                             int             comment '附件数量',
apply_loan_purpose                                     string          comment '申款用途',
apply_loan_type                                        int             comment '申款类型 1借款 2付款 3冲帐',
pay_type                                               int             comment '付款方式 1现金 2支票 3转帐',
payee_full_name                                        string          comment '收款人全称',
open_bank                                              string          comment '开户行',
accounts                                               string          comment '帐号',
cheque_number                                          string          comment '支票号',
note                                                   string          comment '备注',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
company_id                                             int             comment '公司主体id',
company_name                                           string          comment '公司主体名字',
bank_type                                              int             comment '银行类型 1招商 2平安 3其他',
bank_city_code                                         string          comment '银行城市代码',
all_apply_money                                        double          comment '总申请金额',
all_paid_money                                         double          comment '总的已付金额',
refund_type                                            int             comment '退款类型0:不是退款，1:成交单退款2.退流水，默认0',
bank_flow_unverified_amount                            double          comment '流水的待核销金额',
pay_company_id                                         int             comment '付款公司主体id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_payment_approval'
;
