CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_zxs_adjust_cust_see_dtl`(
  `see_id` bigint COMMENT '带看ID', 
  `emp_id` bigint COMMENT '核算咨询师ID', 
  `emp_name` string COMMENT '核算咨询师名称', 
  `emp_mgr_id` bigint COMMENT '核算咨询师主管ID', 
  `emp_mgr_name` string COMMENT '核算咨询师主管名称', 
  `emp_mgr_leader_id` bigint COMMENT '核算咨询师经理ID', 
  `emp_mgr_leader_name` string COMMENT '核算咨询师经理名称', 
  `clue_city_id` bigint COMMENT '线索来源城市ID', 
  `clue_city_name` string COMMENT '线索来源城市名称', 
  `clue_city_seq` string COMMENT '带开城顺序的线索来源城市名称', 
  `adjust_city_id` bigint COMMENT '咨询师核算城市ID', 
  `adjust_city_name` string COMMENT '咨询师核算城市名称', 
  `adjust_city_seq` string COMMENT '带开城顺序的咨询师核算城市名称', 
  `mgr_adjust_city_id` bigint COMMENT '咨询师主管核算城市ID', 
  `mgr_adjust_city_name` string COMMENT '咨询师主管核算城市名称', 
  `mgr_adjust_city_seq` string COMMENT '带开城顺序的咨询师主管核算城市名称', 
  `mgr_leader_adjust_city_id` bigint COMMENT '咨询师经理核算城市ID', 
  `mgr_leader_adjust_city_name` string COMMENT '咨询师经理核算城市名称', 
  `mgr_leader_adjust_city_seq` string COMMENT '带开城顺序的咨询师经理核算城市名称', 
  `adjust_see_num` double COMMENT '核算带看量', 
  `plan_real_begin_date` string COMMENT '计划/实际 带看开始日期:yyyy-MM-dd', 
  `create_date` string COMMENT '创建日期:yyyy-MM-dd', 
  `happen_date` string COMMENT '业务发生日期:yyyy-MM-dd', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_zxs_adjust_cust_see_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='4', 
  'rawDataSize'='96', 
  'totalSize'='4744', 
  'transient_lastDdlTime'='1590511263')
