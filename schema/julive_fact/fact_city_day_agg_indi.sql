drop table if exists julive_fact.fact_city_day_agg_base_indi;
CREATE TABLE julive_fact.fact_city_day_agg_base_indi(
date_str                                           string        COMMENT '日期字符串:yyyy-MM-dd', 
date_str_zh                                        string        COMMENT '日期中文:yyyy年MM月dd日', 
org_id                                             int           comment '公司id',
org_type                                           int           comment '类型 1 居理 2加盟',
org_name                                           string        comment '公司名称',
city_id                                            bigint        COMMENT '城市id', 
city_name                                          string        COMMENT '城市名称', 
city_seq                                           string        COMMENT '带开城顺序城市名称', 
emp_city_id                                        bigint        COMMENT '员工所在城市id', 
emp_city_name                                      string        COMMENT '员工所在城市名称', 
emp_city_seq                                       string        COMMENT '员工所在带开城顺序城市名称', 
city_region                                        string        COMMENT '所属大区', 
city_type                                          string        COMMENT '城市类型：新城 老城_含副区', 
mgr_city                                           string        COMMENT '主城', 
clue_num                                           int           COMMENT '日线索量', 
distribute_num                                     int           COMMENT '日上户量', 
distribute_day_num                                 int           COMMENT '日发生上户日期总数', 
see_num                                            int           COMMENT '日带看量', 
see_project_num                                    int           COMMENT '日带看楼盘量', 
emp_city_see_num                                   int           COMMENT '员工所在城市日带看量',  
emp_city_see_project_num                           int           COMMENT '员工所在城市日带看楼盘量', 
subscribe_contains_cancel_ext_num                  int           COMMENT '日认购量-含退、含外联', 
subscribe_contains_cancel_ext_amt                  decimal(19,4) COMMENT '日认购-含退、含外联GMV', 
subscribe_contains_cancel_ext_income               decimal(19,4) COMMENT '日认购-含退、含外联收入', 
subscribe_contains_cancel_ext_project_num          int           COMMENT '日认购楼盘量-含退、含外联(月破蛋楼盘数)', 
subscribe_contains_ext_amt                         decimal(19,4) COMMENT '日认购-含外联GMV', 
subscribe_contains_ext_income                      decimal(19,4) COMMENT '日认购-含外联收入', 
emp_city_subscribe_contains_cancel_ext_num         int           COMMENT '员工所在城市日认购量-含退、含外联', 
emp_city_subscribe_contains_cancel_ext_amt         decimal(19,4) COMMENT '员工所在城市日认购-含退、含外联GMV', 
emp_city_subscribe_contains_cancel_ext_income      decimal(19,4) COMMENT '员工所在城市日认购-含退、含外联收入', 
emp_city_subscribe_contains_cancel_ext_project_num int           COMMENT '员工所在城市日认购楼盘量-含退、含外联(月破蛋楼盘数)', 
emp_city_subscribe_contains_ext_amt                decimal(19,4) COMMENT '员工所在城市日认购-含外联GMV', 
emp_city_subscribe_contains_ext_income             decimal(19,4) COMMENT '员工所在城市日认购-含外联收入', 
subscribe_cancel_contains_ext_amt                  decimal(19,4) COMMENT '日退认购金额-含外联', 
emp_city_subscribe_cancel_contains_ext_amt         decimal(19,4) COMMENT '员工所在城市日退认购金额-含外联',
subscribe_contains_ext_num                         int           COMMENT '日认购量-含外联', 
emp_city_subscribe_contains_ext_num                int           COMMENT '员工所在城市日认购量-含外联', 
subscribe_cancel_contains_ext_num                  int           COMMENT '日退认购量-含外联', 
subscribe_cancel_contains_ext_income               decimal(19,4) COMMENT '日退认购佣金-含外联', 
emp_city_subscribe_cancel_contains_ext_num         int           COMMENT '员工所在城市日退认购量-含外联', 
emp_city_subscribe_cancel_contains_ext_income      decimal(19,4) COMMENT '员工所在城市日退认购佣金-含外联',
subscribe_coop_num                                 int           COMMENT '净认购量', 
emp_city_subscribe_coop_num                        int           COMMENT '员工所在城市净认购量',
sign_contains_cancel_ext_num                       int           COMMENT '日签约量-含退、含外联', 
sign_contains_cancel_ext_income                    decimal(19,4) COMMENT '日签约-含退、含外联收入', 
sign_contains_ext_income                           decimal(19,4) COMMENT '日签约-不含退、含外联收入', 
sign_contains_ext_num                              int           COMMENT '日签约量-含外联', 
sign_cancel_contains_ext_num                       int           COMMENT '日退签约量-含外联', 
sign_coop_num                                      int           COMMENT '净签约量', 
emp_city_sign_contains_cancel_ext_num              int           COMMENT '员工所在城市日签约量-含退、含外联', 
emp_city_sign_contains_cancel_ext_income           decimal(19,4) COMMENT '员工所在城市日签约-含退、含外联收入', 
emp_city_sign_contains_ext_income                  decimal(19,4) COMMENT '员工所在城市日签约-不含退、含外联收入', 
emp_city_sign_contains_ext_num                     int           COMMENT '员工所在城市日签约量-含外联', 
emp_city_sign_cancel_contains_ext_num              int           COMMENT '员工所在城市日退签约量-含外联', 
emp_city_sign_coop_num                             int           COMMENT '员工所在城市净签约量', 
actual_amt                                decimal(19,4) COMMENT '日回款金额', 
real_workday_num                          int           COMMENT '员工出勤天数', 
from_source                               int           COMMENT '1-自营 2-乌鲁木齐 3-二手房中介',
etl_time                                  string        COMMENT 'ETL跑数时间')
COMMENT '日-城市指标表'
stored as parquet;



drop table if exists julive_fact.fact_wlmq_city_day_agg_indi;
create external table julive_fact.fact_wlmq_city_day_agg_indi
like julive_fact.fact_city_day_agg_base_indi;

drop table if exists julive_fact.fact_esf_city_day_agg_indi;
create external table julive_fact.fact_esf_city_day_agg_indi
like julive_fact.fact_city_day_agg_base_indi;

drop table if exists julive_fact.fact_city_day_agg_indi;
create external table julive_fact.fact_city_day_agg_indi
like julive_fact.fact_city_day_agg_base_indi;

drop table if exists julive_fact.fact_jms_city_day_agg_indi;
create external table julive_fact.fact_jms_city_day_agg_indi
like julive_fact.fact_city_day_agg_base_indi;





