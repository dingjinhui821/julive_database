CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_payment_dtl`(
  `payment_id` bigint COMMENT '回款ID', 
  `forecast_id` bigint COMMENT '预测回款id', 
  `contract_id` bigint COMMENT '合同id', 
  `category_id` bigint COMMENT '合同分类id', 
  `deal_id` bigint COMMENT '成交单id', 
  `clue_id` bigint COMMENT '线索ID', 
  `channel_id` bigint COMMENT '渠道ID', 
  `project_id` bigint COMMENT '楼盘ID', 
  `project_name` string COMMENT '楼盘名称', 
  `city_id` bigint COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序城市名称', 
  `receive_time` string COMMENT '应收日期', 
  `receive_amt` decimal(19,4) COMMENT '应收金额', 
  `actual_time` string COMMENT '实收日期', 
  `actual_amt` decimal(19,4) COMMENT '实收金额', 
  `commission_type` int COMMENT '佣金类型 1前置电商 2 后置返费 3 成交奖', 
  `step` int COMMENT '电商结佣阶段/返费申请阶段', 
  `audit_date` string COMMENT '审核时间', 
  `payback_emp_id` bigint COMMENT '回款负责人ID', 
  `auditor_id` bigint COMMENT '审核人ID', 
  `create_time` int COMMENT '创建时间', 
  `update_time` int COMMENT '更新时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_payment_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'totalSize'='535', 
  'transient_lastDdlTime'='1590544358')
