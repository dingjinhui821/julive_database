drop table julive_fact.fact_refund_pay_fee_detail_dtl;
create table julive_fact.fact_refund_pay_fee_detail_dtl(
pay_datetime                   string COMMENT '收付款时间',
pay_money                      string COMMENT '付款金额', 
creator                        string COMMENT '创建人', 
employee_name                  string COMMENT '员工名字', 
city_id                        string COMMENT '城市id',
city_name                      string COMMENT '城市名称',
bank_flow_number               string COMMENT '银行流水号', 
arrival_time                   string COMMENT '到账时间',
arrival_money                  string COMMENT '到账金额', 
company_name                   string COMMENT '公司主体名字', 
payee_full_name                string COMMENT '收款人全称', 
accounts                       string COMMENT '帐号', 
deal_id                        string COMMENT '成交单id', 
type                           string COMMENT '退款类型0:不是退款，1:成交单退款2.退流水，默认0'          
)
stored as parquet;

