drop table if exists julive_app.app_market_creative_zhuanhua;
create table if not exists julive_app.app_market_creative_zhuanhua( 
report_date                              STRING                      COMMENT '报告日期',   
account_name                             STRING                      COMMENT '账户名称',   
plan_id                                  STRING                      COMMENT '计划ID',   
creative_id                              STRING                      COMMENT '创意ID',
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
plan_name                                STRING                      COMMENT '计划名称',
title                                    STRING                      COMMENT '标题',
rengou_yingshou_time                     DECIMAL(19,4)               COMMENT '认购金额',
rengou_yingshou_uv                       DECIMAL(19,4)               COMMENT '认购金额',
download_num                             BIGINT                      COMMENT '下载量')
COMMENT '创意层级转化时间维度'
stored as parquet;




drop table if exists julive_app.app_market_creative_zhuanhua_plan_not_sh;
create table if not exists julive_app.app_market_creative_zhuanhua_plan_not_sh( 
report_date_a                            STRING                      COMMENT '报告日期', 
plan_name                                STRING                      COMMENT '计划名称', 
plan_name_1                              STRING                      COMMENT '计划名称切割',  
media_type_name                          STRING                      COMMENT '媒体类型', 
plan_id                                  STRING                      COMMENT '计划ID',
account_name                             STRING                      COMMENT '账户名称',   
cost                                     decimal(19,4)               COMMENT '真实消耗', 
sh_cnt_time                              BIGINT                      COMMENT '上户数',
xs_cnt_time                              BIGINT                      COMMENT '线索数',
jh_cnt                                   BIGINT                      COMMENT '激活数')
COMMENT '创意层级计划转化时间维度'
stored as parquet;
