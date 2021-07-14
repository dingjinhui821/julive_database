
drop table if exists julive_fact.fact_market_appstore_city_share;
CREATE EXTERNAL TABLE julive_fact.fact_market_appstore_city_share(
report_date                                         string        COMMENT '报告日期:yyyy-MM-dd', 
city_name                                           string        COMMENT '城市名称', 
media_name                                          string        COMMENT '媒体类型名称',
sem_cost                                            double        COMMENT '前七天sem消耗',
sem_sh_cnt                                          int           COMMENT '前七天sem上户数',
sem_sh_cost                                         double        comment '前七天sem户均消耗',
appstore_sh_cnt                                     int           comment '当天appstore上户数',
city_rate                                           double        comment '城市分摊比例',  
etl_time                                            string        COMMENT 'ETL跑数时间')
COMMENT '媒体报告-应用商城按城市分摊比例'
PARTITIONED BY (`pdate` string)
stored as parquet;