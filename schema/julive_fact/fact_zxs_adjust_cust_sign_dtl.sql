drop table if exists julive_fact.fact_zxs_adjust_cust_sign_base_dtl;
CREATE TABLE julive_fact.fact_zxs_adjust_cust_sign_base_dtl(
sign_id                                bigint        COMMENT '签约ID',
org_id                                 int           comment '公司id',
org_type                               int           comment '类型 1 居理 2加盟',
org_name                               string        comment '公司名称',   
emp_id                                 bigint        COMMENT '核算咨询师ID', 
emp_name                               string        COMMENT '核算咨询师名称', 
emp_mgr_id                             bigint        COMMENT '核算咨询师主管ID', 
emp_mgr_name                           string        COMMENT '核算咨询师主管名称', 
emp_mgr_leader_id                      bigint        COMMENT '核算咨询师经理ID', 
emp_mgr_leader_name                    string        COMMENT '核算咨询师经理名称', 
clue_city_id                           bigint        COMMENT '线索来源城市ID', 
clue_city_name                         string        COMMENT '线索来源城市名称', 
clue_city_seq                          string        COMMENT '带开城顺序的线索来源城市名称', 
adjust_city_id                         bigint        COMMENT '咨询师核算城市ID', 
adjust_city_name                       string        COMMENT '咨询师核算城市名称', 
adjust_city_seq                        string        COMMENT '带开城顺序的咨询师核算城市名称', 
mgr_adjust_city_id                     bigint        COMMENT '咨询师主管核算城市ID', 
mgr_adjust_city_name                   string        COMMENT '咨询师主管核算城市名称', 
mgr_adjust_city_seq                    string        COMMENT '带开城顺序的咨询师主管核算城市名称', 
mgr_leader_adjust_city_id              bigint        COMMENT '咨询师主管核算城市ID', 
mgr_leader_adjust_city_name            string        COMMENT '咨询师主管核算城市名称', 
mgr_leader_adjust_city_seq             string        COMMENT '带开城顺序的咨询师主管核算城市名称', 
sign_status                            int           COMMENT '退化 ：签约状态: 1：已认购  2：退认购', 
sign_type                              int           COMMENT '退化 ：签约类型: 1合作 4外联', 
orig_sign_income                       decimal(19,4) COMMENT '签约原合同预测总收入(佣金)', 
orig_adjust_sign_num                   double        COMMENT '原始核算签约量', 
adjust_sign_contains_cancel_ext_income decimal(19,4) COMMENT '核算签约-含退、含外联收入(佣金)', 
adjust_sign_contains_ext_income        decimal(19,4) COMMENT '核算签约-不含退、含外联收入(佣金)', 
adjust_sign_contains_cancel_income     decimal(19,4) COMMENT '核算签约-含退、不含外联收入(佣金)', 
adjust_sign_coop_income                decimal(19,4) COMMENT '核算签约-合作、不含外联收入(佣金)', 
adjust_sign_contains_cancel_ext_num    double        COMMENT '核算签约量-含退、含外联', 
adjust_sign_contains_ext_num           double        COMMENT '核算签约量-不含退、含外联', 
adjust_sign_contains_cancel_num        double        COMMENT '核算签约量-含退、不含外联', 
adjust_sign_coop_num                   double        COMMENT '核算签约量-合作、不含外联', 
sign_date                              string        COMMENT '认购日期:yyyy-MM-dd', 
create_date                            string        COMMENT '创建日期:yyyy-MM-dd', 
happen_date                            string        COMMENT '业务发生日期:yyyy-MM-dd',
from_source                            int           COMMENT '1-自营,2-乌鲁木齐,3-二手房中介', 
etl_time                               string        COMMENT 'ETL跑数时间')
COMMENT '咨询师签约核算明细表'
stored as parquet;



drop table if exists julive_fact.fact_wlmq_zxs_adjust_cust_sign_dtl;
create external table julive_fact.fact_wlmq_zxs_adjust_cust_sign_dtl
like julive_fact.fact_zxs_adjust_cust_sign_base_dtl;

drop table if exists julive_fact.fact_zxs_adjust_cust_sign_dtl;
create external table julive_fact.fact_zxs_adjust_cust_sign_dtl
like julive_fact.fact_zxs_adjust_cust_sign_base_dtl;

drop table if exists julive_fact.fact_esf_zxs_adjust_cust_sign_dtl;
create external table julive_fact.fact_esf_zxs_adjust_cust_sign_dtl
like julive_fact.fact_zxs_adjust_cust_sign_base_dtl;

drop table if exists julive_fact.fact_jms_zxs_adjust_cust_sign_dtl;
create external table julive_fact.fact_jms_zxs_adjust_cust_sign_dtl
like julive_fact.fact_zxs_adjust_cust_sign_base_dtl;
