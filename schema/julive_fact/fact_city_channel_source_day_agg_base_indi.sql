drop table if exists julive_fact.fact_city_channel_source_day_agg_base_indi;
CREATE TABLE julive_fact.fact_city_channel_source_day_agg_base_indi(
date_str                                            string        COMMENT '日期字符串:yyyy-MM-dd', 
date_str_zh                                         string        COMMENT '日期中文:yyyy年MM月dd日', 
week_type                                           string        COMMENT '周中 周末',
org_id                                              int           comment '公司id',
org_type                                            int           comment '类型 1 居理 2加盟',
org_name                                            string        comment '公司名称', 
region                                              string        COMMENT '所属大区',
bd_region                                           string        COMMENT '渠道大区',
bd_warregion                                        string        COMMENT '渠道战区',
mgr_city_seq                                        string        COMMENT '主城 开城顺序城市名称', 
mgr_city                                            string        COMMENT '主城',
city_id                                             bigint        COMMENT '城市id', 
city_name                                           string        COMMENT '城市名称', 
city_seq                                            string        COMMENT '带开城顺序的城市名称',
emp_city_id                                         bigint        COMMENT '员工所在城市id', 
emp_city_name                                       string        COMMENT '员工所在城市名称', 
emp_city_seq                                        string        COMMENT '员工所在带开城顺序的城市名称',  
channel_id                                          bigint        COMMENT '渠道ID', 
channel_name                                        string        COMMENT '渠道名称', 
source                                              int           COMMENT '订单了解途径', 
source_tc                                           string        COMMENT '用户来源,存储source转码结果',
media_id                                            int           COMMENT '媒体类型ID', 
         
media_name                                          string        COMMENT '媒体类型名称', 
module_id                                           int           COMMENT '模块ID', 
module_name                                         string        COMMENT '模块名称', 
device_id                                           int           COMMENT '设备ID', 
device_name                                         string        COMMENT '设备名称', 
clue_num                                            int           COMMENT '线索量', 
distribute_num                                      int           COMMENT '上户量', 
see_num                                             int           COMMENT '带看量', 
see_project_num                                     int           COMMENT '带看楼盘量', 
emp_city_see_num                                    int           COMMENT '带看量', 
emp_city_see_project_num                            int           COMMENT '带看楼盘量', 
subscribe_contains_cancel_ext_num                   int           COMMENT '认购量-含退、含外联', 
subscribe_contains_cancel_ext_amt                   decimal(19,4) COMMENT '认购-含退、含外联GMV', 
subscribe_contains_cancel_ext_income                decimal(19,4) COMMENT '认购-含退、含外联收入', 
subscribe_contains_cancel_ext_project_num           int           COMMENT '认购楼盘量-含退、含外联(月破蛋楼盘数)', 
subscribe_contains_ext_amt                          decimal(19,4) COMMENT '认购-含外联GMV', 
subscribe_contains_ext_income                       decimal(19,4) COMMENT '认购-含外联收入', 
emp_city_subscribe_contains_cancel_ext_num          int           COMMENT '认购量-含退、含外联', 
emp_city_subscribe_contains_cancel_ext_amt          decimal(19,4) COMMENT '认购-含退、含外联GMV', 
emp_city_subscribe_contains_cancel_ext_income       decimal(19,4) COMMENT '认购-含退、含外联收入', 
emp_city_subscribe_contains_cancel_ext_project_num  int           COMMENT '认购楼盘量-含退、含外联(月破蛋楼盘数)', 
emp_city_subscribe_contains_ext_amt                 decimal(19,4) COMMENT '认购-含外联GMV', 
emp_city_subscribe_contains_ext_income              decimal(19,4) COMMENT '认购-含外联收入', 

subscribe_cancel_contains_ext_amt                   decimal(19,4) COMMENT '退认购金额-含外联', 
emp_city_subscribe_cancel_contains_ext_amt          decimal(19,4) COMMENT '退认购金额-含外联', 

subscribe_contains_ext_num                          int           COMMENT '认购量-含外联',
emp_city_subscribe_contains_ext_num                 int           COMMENT '认购量-含外联', 

subscribe_cancel_contains_ext_num                   int           COMMENT '退认购量-含外联', 
subscribe_cancel_contains_ext_income                decimal(19,4) COMMENT '退认购佣金-含外联',
emp_city_subscribe_cancel_contains_ext_num          int           COMMENT '退认购量-含外联', 
emp_city_subscribe_cancel_contains_ext_income       decimal(19,4) COMMENT '退认购佣金-含外联', 

sign_contains_cancel_ext_num                        int           COMMENT '签约量-含退、含外联', 
sign_contains_cancel_ext_income                     decimal(19,4) COMMENT '签约-含退、含外联收入',
sign_contains_ext_income                            decimal(19,4) COMMENT '签约-含外联收入',  
sign_contains_cancel_ext_amt                        decimal(19,4) COMMENT '签约-含退、含外联金额', 
sign_contains_ext_num                               int           COMMENT '签约量-含外联', 
sign_cancel_contains_ext_num                        int           COMMENT '退签约量-含外联',

emp_city_sign_contains_cancel_ext_num               int           COMMENT '签约量-含退、含外联', 
emp_city_sign_contains_cancel_ext_income            decimal(19,4) COMMENT '签约-含退、含外联收入',
emp_city_sign_contains_ext_income                   decimal(19,4) COMMENT '签约-含外联收入',  
emp_city_sign_contains_cancel_ext_amt               decimal(19,4) COMMENT '签约-含退、含外联金额', 
emp_city_sign_contains_ext_num                      int           COMMENT '签约量-含外联', 
emp_city_sign_cancel_contains_ext_num               int           COMMENT '退签约量-含外联',
from_source                                         int           COMMENT '1-自营数据 2-乌鲁木齐项目数据 3-开发商项目数据 4-加盟商',       
etl_time                                            string        COMMENT 'ETL跑数时间')
COMMENT '日-城市-渠道 指标表'
stored as parquet;

drop table if exists julive_fact.fact_wlmq_city_channel_source_day_agg_indi;
create external table julive_fact.fact_wlmq_city_channel_source_day_agg_indi
like julive_fact.fact_city_channel_source_day_agg_base_indi;

drop table if exists julive_fact.fact_city_channel_source_day_agg_indi;
create external table julive_fact.fact_city_channel_source_day_agg_indi
like julive_fact.fact_city_channel_source_day_agg_base_indi;

drop table if exists julive_fact.fact_esf_city_channel_source_day_agg_indi;
create external table julive_fact.fact_esf_city_channel_source_day_agg_indi
like julive_fact.fact_city_channel_source_day_agg_base_indi;

drop table if exists julive_fact.fact_jms_city_channel_source_day_agg_indi;
create external table julive_fact.fact_jms_city_channel_source_day_agg_indi
like julive_fact.fact_city_channel_source_day_agg_base_indi;

