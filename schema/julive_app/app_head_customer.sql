drop table if exists julive_app.app_head_customer_base;

CREATE TABLE julive_app.app_head_customer_base(
clue_id                      string COMMENT '线索ID', 
tag_creator_id               bigint COMMENT '标记人ID', 
tag_creator_name             string COMMENT '标记人名称', 
now_tag_creator_leader_id    bigint COMMENT '当前标记人直接领导ID', 
now_tag_creator_leader_name  string COMMENT '当前标记人直接领导名称', 
tag_creator_leader_id        bigint COMMENT '业务发生时间标记人直接领导ID', 
tag_creator_leader_name      string COMMENT '业务发生时间标记人直接领导名称', 
tag_create_date              string COMMENT '标记日期:yyyy-MM-dd', 
channel_id                   bigint COMMENT '渠道ID', 
distribute_date              string COMMENT '订单分配日期', 
customer_intent_city_id      int    COMMENT '客户意向城市ID', 
customer_intent_city_name    string COMMENT '客户意向城市名称', 
customer_intent_city_seq     string COMMENT '带开城顺序的客户意向城市名称', 
intent_low_time              string COMMENT '变为无意向的时间:yyyy-MM-dd HH:mm:ss', 
district_id                  string COMMENT '关注区域ID', 
interest_project             string COMMENT '客户感兴趣楼盘ID', 
qualifications               string COMMENT '是否有资质', 
intent_no_like               string COMMENT '无意向说明', 
intent                       int    COMMENT '1.无意向 2.保留 3.有意向', 
intent_tc                    string COMMENT '1.无意向 2.保留 3.有意向', 
total_price_max              string COMMENT '最大总价', 
budget_range_grade           string COMMENT '预算区间：通过最大总价计算', 
create_date                  string COMMENT '线索创建日期:yyyy-MM-dd', 
clue_num                     int    COMMENT '线索量', 
distribute_num               int    COMMENT '上户量', 
see_num                      int    COMMENT '带看量', 
see_project_num              int    COMMENT '带看楼盘量', 
subscribe_num                int    COMMENT '认购量:含退、含外联', 
subscribe_coop_num           int    COMMENT '净认购量', 
sign_num                     int    COMMENT '签约量:含退、含外联', 
call_duration                int    COMMENT '截止当前线索通话时长(秒)', 
call_num                     int    COMMENT '截止当前通话次数', 
from_source                  int    COMMENT '1-自营2乌鲁木齐3二手房中介4-加盟商', 
org_id                       int    comment '公司id',
org_name                     string comment '公司名称', 
etl_time                     string COMMENT 'ETL跑数时间')
COMMENT '头部客户报表'
stored as parquet;



drop table if exists julive_app.app_head_customer;
create table julive_app.app_head_customer like julive_app.app_head_customer_base;

drop table if exists julive_app.app_wlmq_head_customer;
create table  julive_app.app_wlmq_head_customer like julive_app.app_head_customer_base;

drop table if exists julive_app.app_esf_head_customer;
create table  julive_app.app_esf_head_customer like julive_app.app_head_customer_base;

drop table if exists julive_app.app_jms_head_customer;
create table  julive_app.app_jms_head_customer like julive_app.app_head_customer_base;