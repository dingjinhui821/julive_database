drop table if exists julive_fact.fact_grass_sign_base_dtl;
CREATE EXTERNAL TABLE julive_fact.fact_grass_sign_base_dtl(
sign_id                         bigint        COMMENT '草签ID', 
clue_id                         bigint        COMMENT '线索id', 
channel_id                      bigint        COMMENT '渠道ID', 
deal_id                         bigint        COMMENT '成交id', 
subscribe_id                    bigint        COMMENT '认购id', 
emp_id                          bigint        COMMENT '员工id', 
emp_name                        string        COMMENT '员工姓名', 
user_id                         bigint        COMMENT '用户id', 
user_name                       string        COMMENT '用户姓名', 
customer_intent_city_id         int           COMMENT '客户意向城市ID', 
customer_intent_city_name       string        COMMENT '客户意向城市名称', 
customer_intent_city_seq        string        COMMENT '带开城顺序客户意向城市名称', 
project_city_id                 int           COMMENT '草签楼盘所在城市ID', 
project_city_name               string        COMMENT '草签楼盘所在城市名称', 
project_city_seq                string        COMMENT '带开城顺序排号楼盘所在城市名称', 
project_id                      bigint        COMMENT '楼盘id', 
project_name                    string        COMMENT '楼盘名称', 
city_id                         int           COMMENT '城市id', 
city_name                       string        COMMENT '城市名称', 
city_seq                        string        COMMENT '带开城顺序城市名称', 
house_type                      int           COMMENT '户型', 
acreage                         double        COMMENT '面积', 
house_number                    string        COMMENT '房号', 
sign_status                     int           COMMENT '3:已草签 4:退草签 ', 
sign_type                       int           COMMENT '成交类型: 1合作 4外联', 
orig_deal_amt                   decimal(19,4) COMMENT '原成交金额', 
orig_grass_num                  int           COMMENT '草签量--未指定合作和外联', 
orig_grass_contains_cancel_num  int           COMMENT '含退草签--未指定合作和外联', 
grass_coop_num                  int           COMMENT '草签量-不含外联', 
grass_contains_ext_num          int           COMMENT '草签量-含外联', 
grass_coop_cancel_num           int           COMMENT '退草签量-不含外联', 
grass_contains_ext_cancel_num   int           COMMENT '退草签量-含外联', 
grass_sign_time                 string        COMMENT '草签时间', 
create_time                     string        COMMENT '创建时间',
from_source                     int           COMMENT '1-自营2乌鲁木齐3二手房中介4-加盟商', 
org_id                          int           comment '公司id',
org_name                        string        comment '公司名称', 
etl_time                        string        COMMENT 'ETL跑数时间')
COMMENT '草签事实表'
stored as parquet;

drop table if exists julive_fact.fact_grass_sign_dtl;
create table julive_fact.fact_grass_sign_dtl like julive_fact.fact_grass_sign_base_dtl;

drop table if exists julive_fact.fact_wlmq_grass_sign_dtl;
create table  julive_fact.fact_wlmq_grass_sign_dtl like julive_fact.fact_grass_sign_base_dtl;

drop table if exists julive_fact.fact_esf_grass_sign_dtl;
create table  julive_fact.fact_esf_grass_sign_dtl like julive_fact.fact_grass_sign_base_dtl;

drop table if exists julive_fact.fact_jms_grass_sign_dtl;
create table  julive_fact.fact_jms_grass_sign_dtl like julive_fact.fact_grass_sign_base_dtl;




