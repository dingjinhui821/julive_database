CREATE EXTERNAL TABLE `julive_fact.fact_dsp_sem_keyword_day_indi`(
  `city_id` int COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `channel_id` int COMMENT '渠道ID', 
  `channel_name` string COMMENT '渠道名称', 
  `media_id` int COMMENT '媒体ID', 
  `media_name` string COMMENT '媒体名称', 
  `module_id` int COMMENT '模块ID', 
  `module_name` string COMMENT '模块名称', 
  `account_id` int COMMENT '市场投放账户ID', 
  `account_name` string COMMENT '市场投放账户名', 
  `plan_id` bigint COMMENT '推广计划ID', 
  `plan_name` string COMMENT '推广计划', 
  `unit_id` bigint COMMENT '推广单元ID', 
  `unit_name` string COMMENT '推广单元', 
  `keyword_id` bigint COMMENT '关键词ID', 
  `keyword_name` string COMMENT '关键词', 
  `channel_put` string COMMENT '投放渠道:计划|单元|关键词', 
  `match_type` string COMMENT '匹配模式', 
  `pc_average_ranking` string COMMENT 'PC端平均排名', 
  `m_average_ranking` string COMMENT 'M端平均排名', 
  `pc_url` string COMMENT 'PC端关键词投放URL', 
  `m_url` string COMMENT 'M端关键词投放URL', 
  `report_date` string COMMENT '报告日期:yyyy-MM-dd', 
  `show_num` int COMMENT '关键词每天展示次数', 
  `click_num` int COMMENT '关键词每天点击次数', 
  `bill_cost` decimal(15,4) COMMENT '关键词每天账单消费', 
  `cost` decimal(15,4) COMMENT '关键词每天现金消费', 
  `etl_time` string COMMENT 'etl跑数时间')
COMMENT 'DSP-SEM关键词粒度事实表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期:yyyyMMdd', 
  `psrc` string COMMENT '数据来源')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_dsp_sem_keyword_day_indi'
TBLPROPERTIES (
  'transient_lastDdlTime'='1576741720')

