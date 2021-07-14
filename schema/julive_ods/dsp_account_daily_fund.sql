drop table if exists ods.dsp_account_daily_fund;
create external table ods.dsp_account_daily_fund(
id                                                     int             comment '',
cur_date                                               int             comment '日期，存时间戳',
dsp_account_id                                         int             comment 'dsp账户id',
account_name                                           string          comment '账户名',
media_type                                             int             comment '媒体类型  具体定义参考idspconstant',
product_type                                           int             comment '产品形态（feed:1sem:4app:3）',
balance                                                double          comment '账面余额',
cost                                                   double          comment '账面消耗',
charge                                                 double          comment '账面充值',
real_cost                                              double          comment '现金消耗',
real_charge                                            double          comment '现金充值',
real_balance                                           double          comment '现金余额',
avg_cost                                               double          comment '7日平均消耗',
maybe_use_up_days                                      int             comment '余额预计可消费天数',
budget                                                 double          comment '账户预算',
budget_type                                            int             comment '账户预算类型 0 为不设置预算 1 为日预算 2 为周预算',
weekly_budget                                          double          comment '周预算',
region_target                                          string          comment '投放区域',
region_target_code                                     string          comment '投放区域编码',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_account_daily_fund'
;
