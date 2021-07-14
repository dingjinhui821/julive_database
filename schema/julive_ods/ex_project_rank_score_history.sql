drop table if exists ods.ex_project_rank_score_history;
create external table ods.ex_project_rank_score_history(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
score_id                                               int             comment '得分id',
rank                                                   int             comment '排名',
total_score                                            double          comment '楼盘总分',
case_visit_num                                         double          comment '案场到访量',
case_deal_num                                          double          comment '案场成交量',
case_conversion_rate                                   double          comment '案场转化率',
case_visit_num_score                                   double          comment '楼盘案场到访量得分',
case_deal_num_score                                    double          comment '楼盘案场成交量得分',
case_conversion_rate_score                             double          comment '楼盘案场转化率得分',
is_continue_push                                       double          comment '是否有加推 1是 2否',
set_commission                                         double          comment '楼盘套佣',
set_commission_score                                   double          comment '楼盘套佣得分',
payment_peroid                                         double          comment '楼盘回款周期',
payment_peroid_score                                   double          comment '楼盘回款周期得分',
forecast_payment                                       double          comment '合同预测回款金额',
repaid_money                                           double          comment '已回款金额',
repaid_amount                                          double          comment '已回款金额占比',
repaid_peroid                                          double          comment '已回款周期',
unpaid_money                                           double          comment '未回款金额',
unpaid_amount                                          double          comment '未回款金额占比',
unpaid_peroid                                          double          comment '未回款周期',
param_history_id                                       bigint          comment '参数配置历史id',
rank_history_id                                        bigint          comment '楼盘排名配置历史id',
city_id                                                int             comment '城市id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
version_num                                            bigint          comment '版本号',
error_param_json                                       string          comment '错误参数json',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_project_rank_score_history'
;
