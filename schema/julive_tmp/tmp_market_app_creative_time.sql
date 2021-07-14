drop table if exists tmp_etl.creative_report_app;
create table if not exists tmp_etl.creative_report_app( 
report_date                                  STRING                            COMMENT '报告日期',   
account_name                                 STRING                            COMMENT '账户名称',   
dsp_account_id                               STRING                            COMMENT '账户ID',   
plan_id                                      STRING                            COMMENT '计划ID',  
plan_name                                    STRING                            COMMENT '计划名称',
creative_id                                  STRING                            COMMENT '创意ID',
creative_name                                STRING                            COMMENT '创意名称',
title                                        STRING                            COMMENT '标题',
media_type_name                              STRING                            COMMENT '媒体类型',   
app_type                                     STRING                            COMMENT 'APP类型',  
show_num                                     BIGINT                            COMMENT '展示',   
click_num                                    BIGINT                            COMMENT '点击',   
bill_cost                                    decimal(19,4)                     COMMENT '消耗',
cost                                         decimal(19,4)                     COMMENT '真实消耗')
COMMENT 'App创意层级展点消'
stored as parquet;

drop table if exists tmp_etl.plan;
create table tmp_etl.plan(
plan_id                      STRING             COMMENT '计划ID',
plan_name                    STRING             COMMENT '计划名称');

drop table if exists tmp_etl.creative;
create table tmp_etl.creative(
creative_id              STRING             COMMENT '创意ID',
title                    STRING             COMMENT '标题');

drop table if exists tmp_etl.creative_app_jihuo;
create table if not exists tmp_etl.creative_app_jihuo( 
global_id                   STRING             COMMENT '唯一ID',   
city                        STRING             COMMENT '城市',   
utm_source                  STRING             COMMENT '广告来源',   
install_date_time           STRING             COMMENT '安装APP时间',  
create_date                 STRING             COMMENT '创建订单时间',
distribute_date             STRING             COMMENT '上户时间',
first_see_date              STRING             COMMENT '首次带看时间',
first_subscribe_date        STRING             COMMENT '首次认购时间',   
channel_type_name           STRING             COMMENT '渠道类型名称', 
app_id                      STRING             COMMENT 'APP类型',
aid                         STRING             COMMENT '创意ID',    
cid                         STRING             COMMENT '计划ID',
julive_id                   STRING             COMMENT '',
order_id                    STRING             COMMENT '订单ID',
sh_order_id                 STRING             COMMENT '上户订单ID',
dk_order_id                 STRING             COMMENT '带看订单ID',
rg_order_id                 STRING             COMMENT '认购订单ID',
subscribe_contains_cancel_ext_income  DECIMAL(19,4)   COMMENT '认购含退含外联金额')
COMMENT 'app创意相关激活及业务计数'
stored as parquet;

drop table if exists tmp_etl.app_zhuanhua_time;
create table if not exists tmp_etl.app_zhuanhua_time(
report_date		              STRING             COMMENT '报告日期',
media_type		              STRING             COMMENT '媒体类型',
app_id		                  STRING             COMMENT 'app类型',
cid		                      STRING             COMMENT '计划ID',
aid		                      STRING             COMMENT '创意ID',
utm_source		              STRING             COMMENT '广告来源',
customer_intent_city_name	  STRING             COMMENT '客户意向城市',
xs_cnt_time		              BIGINT             COMMENT '线索数',                       ,
sh_cnt_time		              BIGINT             COMMENT '上户次数',
dk_cnt_time		              BIGINT             COMMENT '带看次数',
rg_cnt_time		              BIGINT             COMMENT '认购次数',
rengou_yingshou		          decimal(19,4)      COMMENT '认购金额')
COMMENT '业务计数以及认购金额'
stored as parquet;
;






