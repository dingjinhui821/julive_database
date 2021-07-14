CREATE EXTERNAL TABLE `julive_fact.fact_dsp_sem_keyword_report_hour_dtl`(
  `channel_city_id` int COMMENT '渠道城市ID', 
  `channel_city_name` string COMMENT '渠道城市名称', 
  `url_city_id` int COMMENT 'URL解析城市ID', 
  `url_city_name` string COMMENT 'URL解析城市名称', 
  `kw_city_id` int COMMENT '关键词解析城市ID', 
  `kw_city_name` string COMMENT '关键词解析城市名称', 
  `channel_id` int COMMENT '渠道ID', 
  `channel_name` string COMMENT '渠道名称', 
  `media_id` int COMMENT '媒体ID', 
  `media_name` string COMMENT '媒体名称', 
  `module_id` int COMMENT '模块ID', 
  `module_name` string COMMENT '模块名称', 
  `device_id` int COMMENT '设备ID:0 全部 1 计算机 2 移动设备', 
  `device_name` string COMMENT '设备名称', 
  `account_id` int COMMENT '市场投放账户ID', 
  `account_name` string COMMENT '市场投放账户名', 
  `plan_id` bigint COMMENT '推广计划ID', 
  `plan_name` string COMMENT '推广计划', 
  `unit_id` bigint COMMENT '推广单元ID', 
  `unit_name` string COMMENT '推广单元', 
  `keyword_id` bigint COMMENT '关键词ID', 
  `keyword_name` string COMMENT '关键词', 
  `match_type` int COMMENT '匹配模式(取值范围:1 精确匹配 2 高级短语匹配 3 广泛匹配)', 
  `channel_put` string COMMENT '投放渠道:计划|单元|关键词', 
  `report_date` string COMMENT '报告日期:yyyy-MM-dd', 
  `data_hour` string COMMENT '报告时段:HH', 
  `show_num` int COMMENT '展示次数', 
  `click_num` int COMMENT '点击次数', 
  `bill_cost` decimal(15,4) COMMENT '账面消费', 
  `cost` decimal(15,4) COMMENT '现金消费:由账面消费计算得出', 
  `average_ranking` int COMMENT '平均排名', 
  `price` decimal(15,4) COMMENT '出价', 
  `etl_time` string COMMENT 'etl跑数时间')
COMMENT 'DSP-SEM关键词小时报告明细事实表'
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
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_dsp_sem_keyword_report_hour_dtl'
TBLPROPERTIES (
  'transient_lastDdlTime'='1563084763')
