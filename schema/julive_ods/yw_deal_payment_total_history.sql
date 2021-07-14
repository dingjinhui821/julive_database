drop table if exists ods.yw_deal_payment_total_history;
create external table ods.yw_deal_payment_total_history(
id                                                     bigint          comment 'id',
deal_id                                                bigint          comment '成交单id',
total_id                                               bigint          comment '汇总表id',
actual_count                                           int             comment '实际回款笔数',
manual_count                                           int             comment '人工回款笔数',
actual_money                                           double          comment '实际回款总金额',
manual_money                                           double          comment '人工回款总金额',
manual_week_money                                      double          comment '今日至本周末人工回款总金额',
manual_month_money                                     double          comment '今日至本月底人工回款总金额',
manual_week_money_next                                 double          comment '今日至本周末人工回款总金额(明日)',
manual_month_money_next                                double          comment '今日至本月底人工回款总金额(明日)',
audit_pending                                          int             comment '未审核实收笔数',
audit_yes                                              int             comment '已审核实收笔数',
audit_no                                               int             comment '审核驳回实收笔数',
first_payment_status                                   int             comment '回款跟进状态 1首次人工预测不及时',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
invoiced_money                                         double          comment '已开票总金额',
not_invoiced_money                                     double          comment '未开票总金额',
reviewed_actual_money                                  double          comment '已审核的实际回款总金额',
audit_datetime                                         bigint          comment '成交单回款最新财务审核时间',
forecast_payment_total                                 double          comment '合同预测总佣金',
ecom_money_total                                       double          comment '电商总佣金',
back_money_total                                       double          comment '返费总佣金',
deal_reward_money_total                                double          comment '成交奖总佣金',
is_disconnect                                          int             comment '合同预测与实收是否有断开 1是 2否',
forecast_week_money                                    double          comment '今日至本周末合同预测总金额',
forecast_month_money                                   double          comment '今日至本月底合同预测总金额',
billing_status                                         int             comment '开票状态 1无需开票，2未开票，3已开票',
refund_money                                           double          comment '退款金额',
expected_payment_total                                 double          comment '预计回款总金额',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_deal_payment_total_history'
;
