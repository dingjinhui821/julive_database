drop table if exists ods.ex_forecast_payment;
create external table ods.ex_forecast_payment(
id                                                     bigint          comment 'id',
contract_id                                            bigint          comment '合同id',
category_id                                            bigint          comment '合同分类id',
deal_id                                                bigint          comment '成交id',
pre_paid_datetime                                      int             comment '预收日期',
pre_paid_money                                         string          comment '预收金额',
content                                                string          comment '展示文本',
commission_type                                        int             comment '佣金类型 1前置电商 2 后置返费 3 成交奖',
step                                                   int             comment '电商结佣阶段/返费申请阶段',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
payback_employee_id                                    bigint          comment '回款负责人',
num                                                    int             comment '第几笔回款',
step_num                                               int             comment '佣金类型+阶段相同的第几笔回款',
is_reach_payment                                       int             comment '是否达到回款条件 1是 2否',
time_type                                              int             comment '预测回款时间类型1.预计2.实际3.待定',
reach_payment_datetime                                 int             comment '达到回款条件的时间',
is_finish_payment                                      int             comment '是否完成回佣 0:否 1:是',
is_review                                              int             comment '是否财务审核 0:否 1:是',
payment_through_datetime                               int             comment '最新财务审核通过时间',
verified_money                                         double          comment '已核销金额',
unverified_money                                       double          comment '待核销金额',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_forecast_payment'
;
