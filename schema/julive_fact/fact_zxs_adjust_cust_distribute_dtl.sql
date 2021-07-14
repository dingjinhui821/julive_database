drop table if exists julive_fact.fact_zxs_adjust_cust_distribute_base_dtl;
CREATE TABLE julive_fact.fact_zxs_adjust_cust_distribute_base_dtl(
clue_id                        bigint         COMMENT '线索ID',
org_id                         int            comment '公司id',
org_type                       int            comment '类型 1 居理 2加盟',
org_name                       string         comment '公司名称', 
emp_id                         bigint         COMMENT '核算咨询师ID', 
emp_name                       string         COMMENT '核算咨询师名称', 
emp_mgr_id                     bigint         COMMENT '核算咨询师主管ID', 
emp_mgr_name                   string         COMMENT '核算咨询师主管名称', 
clue_city_id                   bigint         COMMENT '线索来源城市ID', 
clue_city_name                 string         COMMENT '线索来源城市名称', 
clue_city_seq                  string         COMMENT '带开城顺序的线索来源城市名称', 
customer_intent_city_id        bigint         COMMENT '客户意向城市ID', 
customer_intent_city_name      string         COMMENT '客户意向城市名称', 
customer_intent_city_seq       string         COMMENT '带开城顺序的客户意向城市名称', 
adjust_city_id                 bigint         COMMENT '咨询师核算城市ID', 
adjust_city_name               string         COMMENT '咨询师核算城市名称', 
adjust_city_seq                string         COMMENT '带开城顺序的咨询师核算城市名称', 
mgr_adjust_city_id             bigint         COMMENT '咨询师主管核算城市ID', 
mgr_adjust_city_name           string         COMMENT '咨询师主管核算城市名称', 
mgr_adjust_city_seq            string         COMMENT '带开城顺序的咨询师主管核算城市名称', 
adjust_distribute_num          double         COMMENT '核算上户量', 
first_adjust_distribute_num    double         COMMENT '首次核算上户量', 
create_date                    string         COMMENT '创建日期:yyyy-MM-dd', 
happen_date                    string         COMMENT '业务发生日期:yyyy-MM-dd', 
from_source                    int            comment '1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商',            
etl_time                       string         COMMENT 'ETL跑数时间'
)
COMMENT '咨询师核算上户量明细表'
stored as parquet
;


drop table if exists julive_fact.fact_wlmq_zxs_adjust_cust_distribute_dtl;
create external table julive_fact.fact_wlmq_zxs_adjust_cust_distribute_dtl
like julive_fact.fact_zxs_adjust_cust_distribute_base_dtl;

drop table if exists julive_fact.fact_zxs_adjust_cust_distribute_dtl;
create external table julive_fact.fact_zxs_adjust_cust_distribute_dtl
like julive_fact.fact_zxs_adjust_cust_distribute_base_dtl;

drop table if exists julive_fact.fact_esf_zxs_adjust_cust_distribute_dtl;
create external table julive_fact.fact_esf_zxs_adjust_cust_distribute_dtl
like julive_fact.fact_zxs_adjust_cust_distribute_base_dtl;

drop table if exists julive_fact.fact_jms_zxs_adjust_cust_distribute_dtl;
create external table julive_fact.fact_jms_zxs_adjust_cust_distribute_dtl
like julive_fact.fact_zxs_adjust_cust_distribute_base_dtl;
