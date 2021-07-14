drop table if exists julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl;
CREATE TABLE julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl(
subscribe_id                                bigint        COMMENT '认购ID', 
org_id                                      int           comment '公司id',
org_type                                    int           comment '类型 1 居理 2加盟',
org_name                                    string        comment '公司名称', 
emp_id                                      bigint        COMMENT '核算咨询师ID', 
emp_name                                    string        COMMENT '核算咨询师名称', 
emp_mgr_id                                  bigint        COMMENT '核算咨询师主管ID', 
emp_mgr_name                                string        COMMENT '核算咨询师主管名称', 
clue_city_id                                bigint        COMMENT '线索来源城市ID', 
clue_city_name                              string        COMMENT '线索来源城市名称', 
clue_city_seq                               string        COMMENT '带开城顺序的线索来源城市名称', 
adjust_city_id                              bigint        COMMENT '咨询师核算城市ID', 
adjust_city_name                            string        COMMENT '咨询师核算城市名称', 
adjust_city_seq                             string        COMMENT '带开城顺序的咨询师核算城市名称', 
mgr_adjust_city_id                          bigint        COMMENT '咨询师主管核算城市ID', 
mgr_adjust_city_name                        string        COMMENT '咨询师主管核算城市名称', 
mgr_adjust_city_seq                         string        COMMENT '带开城顺序的咨询师主管核算城市名称', 
subscribe_status                            int           COMMENT '退化 ：认购状态: 1：已认购  2：退认购', 
subscribe_type                              int           COMMENT '退化 ：认购类型: 1合作 4外联', 
orig_subsctibe_income                       decimal(19,4) COMMENT '认购原合同预测总收入(佣金)', 
orig_adjust_subscribe_num                   double        COMMENT '原始核算认购量', 
adjust_subscribe_cancel_contains_ext_income decimal(19,4) COMMENT '核算退认购佣金-含外联(佣金)', 
adjust_subscribe_cancel_contains_ext_num    double        COMMENT '核算退认购量-含外联', 
back_date                                   string        COMMENT '退认购日期:yyyy-MM-dd',
from_source                                 int           COMMENT '1-自营 2-乌鲁木齐 3-二手房中介', 
etl_time                                    string        COMMENT 'ETL跑数时间')
COMMENT '咨询师核算退认购明细表'
stored as parquet;

drop table if exists julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_cancel_dtl;
create external table julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_cancel_dtl
like julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl;
drop table if exists julive_fact.fact_zxs_adjust_cust_subscribe_cancel_dtl;
create external table julive_fact.fact_zxs_adjust_cust_subscribe_cancel_dtl
like julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl;
drop table if exists julive_fact.fact_esf_zxs_adjust_cust_subscribe_cancel_dtl;
create external table julive_fact.fact_esf_zxs_adjust_cust_subscribe_cancel_dtl
like julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl;
drop table if exists julive_fact.fact_jms_zxs_adjust_cust_subscribe_cancel_dtl;
create external table julive_fact.fact_jms_zxs_adjust_cust_subscribe_cancel_dtl
like julive_fact.fact_zxs_adjust_cust_subscribe_cancel_base_dtl;

