
drop table if exists julive_fact.fact_sign_base_dtl;
create external table julive_fact.fact_sign_base_dtl(

sign_id                                bigint         comment '签约ID',
org_id                                 int            comment '公司id',
org_type                               int            comment '类型 1 居理 2加盟',
org_name                               string         comment '公司名称',
deal_id                                bigint         comment '交易ID',
clue_id                                bigint         comment '线索ID',
channel_id                             bigint         comment '渠道ID', -- 20191015 
subscribe_id                           bigint         comment '认购ID',
emp_id                                 bigint         comment '员工ID',
emp_name                               string         comment '员工姓名',
emp_mgr_id                             bigint         comment '咨询师主管id',
emp_mgr_name                           string         comment '咨询师主管姓名',
region                                 string         comment '大区',
mgr_city_seq                           string         comment '主城 开城顺序城市名称',
mgr_city                               string         comment '主城',
city_id                                int            comment '城市ID',
city_name                              string         comment '城市名称',
city_seq                               string         comment '带开城顺序的城市名称',

customer_intent_city_id                int            comment '客户意向城市ID', -- 20191009新加 
customer_intent_city_name              string         comment '客户意向城市名称', -- 20191009新加 
customer_intent_city_seq               string         comment '带开城顺序客户意向城市名称', -- 20191009新加 
source                                 int            COMMENT '订单了解途径', 
source_tc                              string         COMMENT '用户来源,存储source转码结果',

project_city_id                        int            comment '签约楼盘所在城市ID', -- 20191009新加 
project_city_name                      string         comment '签约楼盘所在城市名称', -- 20191009新加 
project_city_seq                       string         comment '带开城顺序签约楼盘所在城市名称', -- 20191009新加 


emp_region                             string         comment '员工所在大区',
emp_mgr_city_seq                       string         comment '员工所在城市主城seq',
emp_mgr_city                           string         comment '员工所在主城名称',
emp_city_id                            string         comment '员工所在主城id',
emp_city_name                          string         comment '员工所在城市名称',
emp_city_seq                           string         comment '带开城顺序的员工所在城市名称',


user_id                                bigint         comment '用户ID',
user_name                              bigint         comment '用户名称',
project_id                             bigint         comment '楼盘ID',
project_name                           string         comment '楼盘名称',

house_type                             string         comment '户型',
acreage                                string         comment '面积',
house_number                           string         comment '房号',

sign_status                            string         comment '签约状态:1已网签 2退网签',
sign_type                              int            comment '成交类型:1合作 4外联',
audit_status                           int            comment '审核状态',
subscribe_status                       int            comment '认购单状态', -- 20191016添加 
subscribe_date                         string         comment '日购日期', -- 20191016添加 

-- 原始指标 
orig_deal_amt                          decimal(19,4)  COMMENT '原签约金额', 
orig_sign_income                       decimal(19,4)  COMMENT '原合同预测总收入',
orig_sign_num                          int            COMMENT '签约量--未指定合作和外联', 
orig_sign_contains_cancel_num          int            COMMENT '含退签约量--未指定合作和外联', 

-- 金额 
sign_contains_cancel_ext_amt           decimal(19,4)  comment '签约-含退、含外联金额',  
sign_contains_cancel_ext_income        decimal(19,4)  comment '签约-含退、含外联收入', 
sign_contains_cancel_amt               decimal(19,4)  comment '签约-含退、不含外联金额',  
sign_contains_cancel_income            decimal(19,4)  comment '签约-含退、不含外联收入', 
sign_contains_ext_amt                  decimal(19,4)  comment '签约-含外联金额',  
sign_contains_ext_income               decimal(19,4)  comment '签约-含外联收入', 
sign_coop_amt                          decimal(19,4)  comment '签约-不含外联金额',  
sign_coop_income                       decimal(19,4)  comment '签约-不含外联收入', 
sign_cancel_contains_ext_amt           decimal(19,4)  comment '退签约金额-含外联', 
sign_cancel_contains_ext_income        decimal(19,4)  comment '退签约收入-含外联', 
sign_cancel_coop_amt                   decimal(19,4)  comment '退签约金额-不含外联', 

-- 量 
sign_contains_cancel_ext_num           int            comment '签约量-含退、含外联',
sign_contains_cancel_num               int            comment '签约量-含退、不含外联',
sign_coop_num                          int            comment '签约量-不含外联',
sign_contains_ext_num                  int            comment '签约量-含外联',
sign_cancel_coop_num                   int            comment '退签约量-不含外联', 
sign_cancel_contains_ext_num           int            comment '退签约量-含外联',

audit_num                              int            comment '审核数量',
sign_time                              string         comment '签约时间',
submit_review_time                     string         comment '提交审核时间',
from_source                            int            comment '1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商',
create_time                            string         comment '创建时间',
etl_time                               string         comment 'ETL跑数时间'

) comment "签约明细事实基础表表" 
stored as parquet
;


drop table if exists julive_fact.fact_sign_dtl_base;
create external table julive_fact.fact_sign_dtl_base
like julive_fact.fact_sign_base_dtl;

drop table if exists julive_fact.fact_sign_dtl;
create external table julive_fact.fact_sign_dtl
like julive_fact.fact_sign_base_dtl;

drop table if exists julive_fact.fact_wlmq_sign_dtl;
create external table julive_fact.fact_wlmq_sign_dtl
like julive_fact.fact_sign_base_dtl;

drop table if exists julive_fact.fact_esf_sign_dtl;
create external table julive_fact.fact_esf_sign_dtl
like julive_fact.fact_sign_base_dtl;
drop table if exists julive_fact.fact_jms_sign_dtl;
create external table julive_fact.fact_jms_sign_dtl
like julive_fact.fact_sign_base_dtl;