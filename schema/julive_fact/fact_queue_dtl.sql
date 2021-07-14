CREATE EXTERNAL TABLE `julive_fact.fact_queue_dtl`(
  `queue_id` bigint COMMENT '排号ID', 
  `clue_id` bigint COMMENT '线索id', 
  `channel_id` bigint COMMENT '渠道ID', 
  `deal_id` bigint COMMENT '成交id', 
  `emp_id` bigint COMMENT '员工id', 
  `emp_name` string COMMENT '员工姓名', 
  `emp_mgr_id` bigint COMMENT '咨询师主管id', 
  `emp_mgr_name` string COMMENT '咨询师主管姓名', 
  `user_id` bigint COMMENT '用户id', 
  `user_name` string COMMENT '用户姓名', 
  `project_id` bigint COMMENT '楼盘id', 
  `project_name` string COMMENT '楼盘名称', 
  `city_id` int COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序城市名称', 
  `customer_intent_city_id` int COMMENT '客户意向城市ID', 
  `customer_intent_city_name` string COMMENT '客户意向城市名称', 
  `customer_intent_city_seq` string COMMENT '带开城顺序客户意向城市名称', 
  `project_city_id` int COMMENT '排号楼盘所在城市id', 
  `project_city_name` string COMMENT '排号楼盘所在城市名称', 
  `project_city_seq` string COMMENT '带开城顺序排号楼盘所在城市名称', 
  `house_type` int COMMENT '户型', 
  `acreage` double COMMENT '面积', 
  `house_number` string COMMENT '房号', 
  `queue_status` int COMMENT '排号状态: 3:已排号 4:退排号', 
  `queue_type` int COMMENT '成交类型 1合作 4外联', 
  `orig_deal_amt` decimal(19,4) COMMENT '原成交金额', 
  `discount` string COMMENT '原排号优惠', 
  `cost` int COMMENT '原排号费用', 
  `orig_queue_num` int COMMENT '排卡量--未指定合作和外联', 
  `orig_queue_contains_cancel_num` int COMMENT '含退排卡--未指定合作和外联', 
  `queue_coop_num` int COMMENT '排卡量-不含外联', 
  `queue_contains_ext_num` int COMMENT '排卡量-含外联', 
  `queue_coop_cancel_num` int COMMENT '退排卡量-不含外联', 
  `queue_contains_ext_cancel_num` int COMMENT '退排卡量-含外联', 
  `queue_time` string COMMENT '排号时间', 
  `create_time` string COMMENT '创建时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '排卡事实表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_queue_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='6293', 
  'rawDataSize'='239134', 
  'totalSize'='715905', 
  'transient_lastDdlTime'='1590544323')
