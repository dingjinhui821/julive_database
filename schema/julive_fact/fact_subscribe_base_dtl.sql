drop table if exists julive_fact.fact_subscribe_base_dtl;
create external table julive_fact.fact_subscribe_base_dtl(
subscribe_id                                      bigint                  comment '认购id',
clue_id                                           bigint                  comment '线索id',
see_id                                            bigint                  comment '带看id',
org_id                                            int                     comment '公司id',
org_type                                          int                     comment '类型 1 居理 2加盟',
org_name                                          string                  comment '公司名称',
channel_id                                        bigint                  comment '渠道ID',
deal_id                                           bigint                  comment '成交id',
project_id                                        bigint                  comment '楼盘id',
project_name                                      string                  comment '楼盘名称',
emp_id                                            bigint                  comment '员工id',
emp_name                                          string                  comment '员工名称',
user_id                                           bigint                  comment '用户id',
user_name                                         string                  comment '用户名称',
region                                            string                  comment '楼盘地理城市所在大区',
mgr_city_seq                                      string                  comment '楼盘地理城市所在主城 开城顺序城市名称',
mgr_city                                          string                  comment '楼盘地理城市所在 主城',
city_id                                           bigint                  comment '线索城市id',
city_name                                         string                  comment '线索城市名称',
city_seq                                          string                  comment '带开城顺序城市名称',
customer_intent_city_id                           int                     comment '客户意向城市ID',
customer_intent_city_name                         string                  comment '客户意向城市名称',
customer_intent_city_seq                          string                  comment '带开城顺序客户意向城市名称',
source                                            int                     comment '订单了解途径',
source_tc                                         string                  comment '用户来源,存储source转码结果',
project_city_id                                   int                     comment '认购楼盘所在城市ID',
project_city_name                                 string                  comment '认购楼盘所在城市名称',
project_city_seq                                  string                  comment '带开城顺序认购楼盘所在城市名称',

emp_region                                        string                  comment '员工所在大区',
emp_mgr_city_seq                                  string                  comment '员工所在城市主城seq',
emp_mgr_city                                      string                  comment '员工所在主城名称',
emp_city_id                                       string                  comment '员工所在主城id',
emp_city_name                                     string                  comment '员工所在城市名称',
emp_city_seq                                      string                  comment '带开城顺序的员工所在城市名称',

subscribe_status                                  int                     comment '退化 ：认购状态: 1：已认购  2：退认购',
subscribe_type                                    int                     comment '退化 ：认购类型: 1合作 4外联',
is_first_see                                      int                     comment '是否首付访认购:1是 0否',
is_first_see_project                              int                     comment '是否订单-楼盘首付访认购:1是 0否',
orig_deal_amt                                     decimal(19,4)           comment '原成交金额',
orig_subsctibe_income                             decimal(19,4)           comment '原合同预测总收入',
orig_subscribe_num                                int                     comment '认购量--未指定合作和外联',
orig_subscribe_contains_cancel_num                int                     comment '含退认购--未指定合作和外联',
subscribe_contains_cancel_ext_amt                 decimal(19,4)           comment '认购-含退、含外联GMV',
subscribe_contains_cancel_ext_income              decimal(19,4)           comment '认购-含退、含外联收入',
subscribe_contains_ext_amt                        decimal(19,4)           comment '认购-含外联GMV',
subscribe_contains_ext_income                     decimal(19,4)           comment '认购-含外联收入',
subscribe_coop_amt                                decimal(19,4)           comment '认购-不含外联GMV',
subscribe_coop_income                             decimal(19,4)           comment '认购-不含外联收入',
subscribe_cancel_contains_ext_amt                 decimal(19,4)           comment '退认购金额-含外联',
subscribe_cancel_contains_ext_income              decimal(19,4)           comment '退认购佣金-含外联',
subscribe_cancel_coop_amt                         decimal(19,4)           comment '退认购金额-不含外联',
subscribe_contains_cancel_ext_num                 int                     comment '认购量-含退、含外联',
subscribe_contains_cancel_num                     int                     comment '认购量-含退、不含外联',
subscribe_coop_num                                int                     comment '认购量-不含外联',
subscribe_contains_ext_num                        int                     comment '认购量-含外联',
subscribe_cancel_coop_num                         int                     comment '退认购量-不含外联',
subscribe_cancel_contains_ext_num                 int                     comment '退认购量-含外联',
subscribe_time                                    string                  comment '认购时间',
back_time                                         string                  comment '退认购单时间',
from_source                                       int                     comment '1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据',
create_time                                       string                  comment '创建时间',
etl_time                                          string                  comment 'ETL跑数时间'
) comment '认购数据基表' 
stored as parquet 
;


-- 子表 
drop table if exists julive_fact.fact_wlmq_subscribe_dtl;
CREATE external TABLE julive_fact.fact_wlmq_subscribe_dtl LIKE julive_fact.fact_subscribe_base_dtl;

drop table if exists julive_fact.fact_subscribe_dtl;
create external table julive_fact.fact_subscribe_dtl like julive_fact.fact_subscribe_base_dtl;

drop table if exists julive_fact.fact_esf_subscribe_dtl;
create external table julive_fact.fact_esf_subscribe_dtl like julive_fact.fact_subscribe_base_dtl;

drop table if exists julive_fact.fact_jms_subscribe_dtl;
create external table julive_fact.fact_jms_subscribe_dtl like julive_fact.fact_subscribe_base_dtl;
 