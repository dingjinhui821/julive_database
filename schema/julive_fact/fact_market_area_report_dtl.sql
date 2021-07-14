drop table if exists julive_fact.fact_market_area_report_dtl;
CREATE EXTERNAL TABLE julive_fact.fact_market_area_report_dtl(
report_date                                         string        COMMENT '报告日期:yyyy-MM-dd', 
report_time                                         string        COMMENT '报告日期:yyyy-MM-dd HH:mm:ss', 
data_from                                           string        COMMENT '数据来源表名',
media_class                                         string        COMMENT '媒体类型分类:feed 应用市场 SEM',
device_name                                         string        COMMENT '设备类型:APP渠道 PC M',
account_id                                          int           COMMENT '帐号id',
show_num                                            int           comment '展示数量',
click_num                                           int           comment '点击数量',
bill_cost                                           double        comment '账单消耗', 
reward                                              double        comment '实际返券', 
cost                                                double        COMMENT '实际消耗',
download_num                                        int           COMMENT '下载量',
plan_id                                             bigint        COMMENT '计划id',
plan_name                                           string        COMMENT '计划名称', 
city_name                                           string        COMMENT '城市名称(SEM 依次取: url城市名 渠道城市名)', 
channel_id                                          int           COMMENT 'SEM渠道id',
channel_name                                        string        COMMENT 'SEM渠道名',
channel_city_name                                   string        COMMENT 'SEM渠道城市名',
url_city_name                                       string        COMMENT 'SEMurl城市名',
kw_city_name                                        string        COMMENT 'SEM空值',
app_type                                            string        COMMENT '设备类型:苹果 安卓 PC M 小程序',
creator                                             int           COMMENT '创建人',
updator                                             int           COMMENT '修改人', 
create_datetime                                     int           COMMENT '创建时间', 
update_datetime                                     int           COMMENT '修改时间',  
etl_time                                            string        COMMENT 'ETL跑数时间')
COMMENT 'FACT-市场媒体地域报告'
PARTITIONED BY (`pdate` string,source string)
stored as parquet;

