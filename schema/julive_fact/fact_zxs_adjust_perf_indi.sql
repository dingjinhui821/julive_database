drop table if exists julive_fact.fact_zxs_adjust_perf_base_indi;
CREATE TABLE julive_fact.fact_zxs_adjust_perf_base_indi(
emp_id                                          bigint        COMMENT '咨询师ID', 
emp_name                                        string        COMMENT '咨询师名称',
org_id                                          int           comment '公司id',
org_type                                        int           comment '类型 1 居理 2加盟',
org_name                                        string        comment '公司名称',  
adjust_city_id                                  bigint        COMMENT '核算城市ID', 
adjust_city_name                                string        COMMENT '核算城市名称', 
adjust_city_seq                                 string        COMMENT '核算城市名称', 
city_region                                     string        COMMENT '所属大区', 
city_type                                       string        COMMENT '城市类型：新城 老城', 
mgr_city                                        string        COMMENT '主城', 
zxs_adjust_city_id                              bigint        COMMENT '咨询师维度表标识核算城市ID', 
zxs_adjust_city_name                            string        COMMENT '咨询师维度表标识核算城市名称', 
zxs_adjust_city_seq                             string        COMMENT '带开城顺序得咨询师维度表标识核算城市名称', 
happen_date                                     string        COMMENT '业务发生日期:yyyy-MM-dd', 
entry_date                                      string        COMMENT '入职日期', 
full_date                                       string        COMMENT '转正日期', 
full_type                                       int           COMMENT '转正状态', 
offjob_date                                     string        COMMENT '离职日期', 
post_id                                         int           COMMENT '岗位ID', 
post_name                                       string        COMMENT '岗位名称', 
dept_id                                         int           COMMENT '部门ID', 
dept_name                                       string        COMMENT '部门名称', 
direct_leader_id                                int           COMMENT '业务发生时主管ID', 
direct_leader_name                              string        COMMENT '业务发生时主管名称', 
indirect_leader_id                              int           COMMENT '业务发生时经理ID', 
indirect_leader_name                            string        COMMENT '业务发生时经理名称', 
now_direct_leader_id                            int           COMMENT '当前主管ID', 
now_direct_leader_name                          string        COMMENT '当前主管名称', 
now_indirect_leader_id                          int           COMMENT '当前经理ID', 
now_indirect_leader_name                        string        COMMENT '当前经理名称', 
promotion_date                                  string        COMMENT '晋升主管日期:yyyy-MM-dd', 
adjust_distribute_num                           double        COMMENT '核算上户量', 
first_adjust_distribute_num                     double        COMMENT '首次核算上户量', 
adjust_see_num                                  double        COMMENT '核算带看量', 
adjust_subscribe_contains_cancel_ext_income     decimal(19,4) COMMENT '核算认购-含退、含外联收入(佣金)', 
adjust_subscribe_contains_ext_income            decimal(19,4) COMMENT '核算认购-不含退、含外联收入(佣金)', 
adjust_subscribe_contains_cancel_income         decimal(19,4) COMMENT '核算认购-含退、不含外联收入(佣金)', 
adjust_subscribe_coop_income                    decimal(19,4) COMMENT '核算认购-合作、不含外联收入(佣金)', 
adjust_subscribe_contains_cancel_ext_num        double        COMMENT '核算认购量-含退、含外联', 
adjust_subscribe_contains_ext_num               double        COMMENT '核算认购量-不含退、含外联', 
adjust_subscribe_contains_cancel_num            double        COMMENT '核算认购量-含退、不含外联', 
adjust_subscribe_coop_num                       double        COMMENT '核算认购量-合作、不含外联', 
adjust_subscribe_cancel_contains_ext_income     decimal(19,4) COMMENT '核算退认购佣金-含外联(佣金)', 
adjust_subscribe_cancel_contains_ext_num        double        COMMENT '核算退认购量-含外联', 
adjust_sign_contains_cancel_ext_income          decimal(19,4) COMMENT '核算签约-含退、含外联收入(佣金)', 
adjust_sign_contains_ext_income                 decimal(19,4) COMMENT '核算签约-不含退、含外联收入(佣金)', 
adjust_sign_contains_cancel_income              decimal(19,4) COMMENT '核算签约-含退、不含外联收入(佣金)', 
adjust_sign_coop_income                         decimal(19,4) COMMENT '核算签约-合作、不含外联收入(佣金)', 
adjust_sign_contains_cancel_ext_num             double        COMMENT '核算签约量-含退、含外联', 
adjust_sign_contains_ext_num                    double        COMMENT '核算签约量-不含退、含外联', 
adjust_sign_contains_cancel_num                 double        COMMENT '核算签约量-含退、不含外联', 
adjust_sign_coop_num                            double        COMMENT '核算签约量-合作、不含外联',
from_source                                     int           COMMENT '1-自营,2-乌鲁木齐,3-二手房中介', 
etl_time                                        string        COMMENT 'ETL跑数时间')
COMMENT '咨询师核算性能指标表'
stored as parquet;



drop table if exists julive_fact.fact_wlmq_zxs_adjust_perf_indi;
create external table julive_fact.fact_wlmq_zxs_adjust_perf_indi
like julive_fact.fact_zxs_adjust_perf_base_indi;

drop table if exists julive_fact.fact_zxs_adjust_perf_indi;
create external table julive_fact.fact_zxs_adjust_perf_indi
like julive_fact.fact_zxs_adjust_perf_base_indi;

drop table if exists julive_fact.fact_esf_zxs_adjust_perf_indi;
create external table julive_fact.fact_esf_zxs_adjust_perf_indi
like julive_fact.fact_zxs_adjust_perf_base_indi;

drop table if exists julive_fact.fact_jms_zxs_adjust_perf_indi;
create external table julive_fact.fact_jms_zxs_adjust_perf_indi
like julive_fact.fact_zxs_adjust_perf_base_indi;
