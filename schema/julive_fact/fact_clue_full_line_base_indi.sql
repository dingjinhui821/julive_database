drop table if exists julive_fact.fact_clue_full_line_base_indi;
CREATE EXTERNAL TABLE  julive_fact.fact_clue_full_line_base_indi (
clue_id                                bigint        COMMENT '线索ID：主键', 
org_id                                 int           comment '公司id',
org_type                               int           comment '类型 1 居理 2加盟',
org_name                               string        comment '公司名称', 
developer_id                           int           COMMENT '订单分配开发商ID,订单来源kfs_order 有效', 
project_id                             int           COMMENT '订单分配开发商对应楼盘ID,订单来源kfs_order 有效', 
op_type                                int           COMMENT 'app留电口', 
op_type_name_cn                        string        COMMENT 'app留电口描述',                                 
city_id                                int           COMMENT '线索来源城市ID', 
city_name                              string        COMMENT '线索来源城市名称', 
city_seq                               string        COMMENT '带开城顺序的线索来源城市名称', 
customer_intent_city_id                int           COMMENT '客户意向城市ID', 
customer_intent_city_name              string        COMMENT '客户意向城市名称', 
customer_intent_city_seq               string        COMMENT '带开城顺序客户意向城市名称', 
first_customer_intent_city_id          int           COMMENT '客户第一意向城市ID', 
first_customer_intent_city_name        string        COMMENT '客户第一意向城市名称', 
first_customer_intent_city_seq         string        COMMENT '带开城顺序客户第一意向城市名称', 
source                                 int           COMMENT '用户来源ID', 
source_tc                              string        COMMENT '用户来源描述', 
user_id                                bigint        COMMENT '用户ID', 
user_name                              string        COMMENT '用户来源',
user_mobile                            string        COMMENT '用户手机号', 
is_distribute                          int           COMMENT '分配方式',
emp_id                                 int           COMMENT '员工id', 
emp_name                               string        COMMENT '员工姓名',  
channel_id                             int           COMMENT '渠道ID', 
channel_city_id                        int           COMMENT '渠道城市ID', 
channel_city_name                      string        COMMENT '渠道城市名称', 
channel_city_seq                       string        COMMENT '带开城顺序渠道城市名称', 
media_id                               int           COMMENT '媒体ID', 
media_name                             string        COMMENT '媒体名称', 
module_id                              int           COMMENT '模块ID', 
module_name                            string        COMMENT '模块名称', 
device_id                              int           COMMENT '设备ID', 
device_name                            string        COMMENT '设备名称', 
create_date                            string        COMMENT '线索创建日期:yyyy-MM-dd', 
distribute_date                        string        COMMENT '上户日期:yyyy-MM-dd', 
distribute_time                        string        COMMENT '上户时间:yyyy-MM-dd HH:mm:ss', 
first_see_date                         string        COMMENT '首次带看日期:yyyy-MM-dd', 
final_see_date                         string        COMMENT '最后带看日期:yyyy-MM-dd',
first_subscribe_date                   string        COMMENT '首次认购日期:yyyy-MM-dd', 
final_subscribe_date                   string        COMMENT '最后认购日期:yyyy-MM-dd',
first_sign_date                        string        COMMENT '首次签约日期:yyyy-MM-dd', 
final_sign_date                        string        COMMENT '最后签约日期:yyyy-MM-dd',
clue_num                               int           COMMENT '线索量', 
distribute_num                         int           COMMENT '上户量', 
see_num                                int           COMMENT '带看量', 
see_project_num                        int           COMMENT '带看楼盘量', 
subscribe_num                          int           COMMENT '认购量:含退、含外联', 
subscribe_coop_num                     int           COMMENT '净认购量:不含退、不含外联', 
subscribe_contains_cancel_ext_num      int           COMMENT '认购量-含退、含外联(同subscribe_num属性)', 
subscribe_contains_ext_num             int           COMMENT '认购量(不含退)-含外联', 
subscribe_contains_ext_amt             decimal(19,4) COMMENT '认购(不含退)-含外联GMV', 
subscribe_contains_ext_income          decimal(19,4) COMMENT '认购(不含退)-含外联佣金', 
subscribe_contains_cancel_ext_amt      decimal(19,4) COMMENT '认购-含退、含外联GMV', 
subscribe_contains_cancel_ext_income   decimal(19,4) COMMENT '认购-含退、含外联收入', 
sign_num                               int           COMMENT '签约量:含退、含外联', 
call_duration                          int           COMMENT '线索通话时长(秒)', 
call_num                               int           COMMENT '线索通话次数', 
clue_id_list                           string        COMMENT '线索ID明细列表', 
distribute_id_list                     string        COMMENT '上户ID明细列表', 
see_id_list                            string        COMMENT '带看ID明细列表', 
see_project_id_list                    string        COMMENT '带看楼盘ID明细列表', 
clue_see_list                          string        COMMENT '产生带看的线索ID明细列表', 
subscribe_contains_cancel_ext_id_list  string        COMMENT '含退含外联认购ID明细列表', 
clue_subscribe_list                    string        COMMENT '产生认购的线索ID明细列表', 
sign_contains_cancel_ext_id_list       string        COMMENT '含退含外联签约ID明细列表', 
clue_sign_list                         string        COMMENT '产生签约的线索ID明细列表', 
clue_see_num                           int           COMMENT '产生带看的线索量', 
clue_subscribe_num                     int           COMMENT '产生认购的线索量', 
clue_sign_num                          int           COMMENT '产生签约的线索量', 
create_time                            string        COMMENT '线索创建时间',
from_source                            int           COMMENT '1-自营2乌鲁木齐3二手房中介', 
etl_time                               string        COMMENT 'ETL跑数时间')
COMMENT '线下订单粒度及后续转化明细指标表'
stored as parquet;


drop table if exists julive_fact.fact_wlmq_clue_full_line_indi;
create table julive_fact.fact_wlmq_clue_full_line_indi 
like julive_fact.fact_clue_full_line_base_indi;

drop table if exists julive_fact.fact_esf_clue_full_line_indi;
create table julive_fact.fact_esf_clue_full_line_indi 
like julive_fact.fact_clue_full_line_base_indi;

drop table if exists julive_fact.fact_clue_full_line_indi;
create table julive_fact.fact_clue_full_line_indi 
like julive_fact.fact_clue_full_line_base_indi;

drop table if exists julive_fact.fact_kfsclue_full_line_indi ;
create table julive_fact.fact_kfsclue_full_line_indi 
like julive_fact.fact_clue_full_line_base_indi;

drop table if exists julive_fact.fact_jms_clue_full_line_indi ;
create table julive_fact.fact_jms_clue_full_line_indi 
like julive_fact.fact_clue_full_line_base_indi;