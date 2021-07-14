drop table if exists julive_app.app_market_plan_area_zhuanhua;
create table if not exists julive_app.app_market_plan_area_zhuanhua( 
report_date                              STRING                      COMMENT '报告日期',   
account_name                             STRING                      COMMENT '账户名称',   
plan_id                                  STRING                      COMMENT '计划ID',   
city                                     STRING                      COMMENT '投放城市',  
media_type_name                          STRING                      COMMENT '媒体类型',   
app_type                                 STRING                      COMMENT 'APP类型',   
show_num                                 BIGINT                      COMMENT '展示',   
click_num                                BIGINT                      COMMENT '点击',   
bill_cost                                decimal(19,4)               COMMENT '消耗',   
cost                                     decimal(19,4)               COMMENT '真实消耗',   
jh_cnt                                   BIGINT                      COMMENT '激活数',   
xs_cnt                                   BIGINT                      COMMENT '线索数',   
sh_cnt                                   BIGINT                      COMMENT '上户数',   
dk_cnt                                   BIGINT                      COMMENT '带看数',   
rg_cnt                                   decimal(19,4)               COMMENT '认购金额',   
xs_cnt_time                              BIGINT                      COMMENT '线索数',   
sh_cnt_time                              BIGINT                      COMMENT '上户数',   
dk_cnt_time                              BIGINT                      COMMENT '带看数',   
rg_cnt_time                              BIGINT                      COMMENT '认购金额',   
plan_name                                STRING                      COMMENT '计划名称')
COMMENT '今日头条带城市统计数据'
stored as parquet; 