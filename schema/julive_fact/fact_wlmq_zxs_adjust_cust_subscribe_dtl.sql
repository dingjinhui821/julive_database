CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_zxs_adjust_cust_subscribe_dtl`(
  `subscribe_id` bigint COMMENT '认购ID', 
  `emp_id` bigint COMMENT '核算咨询师ID', 
  `emp_name` string COMMENT '核算咨询师名称', 
  `emp_mgr_id` bigint COMMENT '核算咨询师主管ID', 
  `emp_mgr_name` string COMMENT '核算咨询师主管名称', 
  `clue_city_id` bigint COMMENT '线索来源城市ID', 
  `clue_city_name` string COMMENT '线索来源城市名称', 
  `clue_city_seq` string COMMENT '带开城顺序的线索来源城市名称', 
  `adjust_city_id` bigint COMMENT '咨询师核算城市ID', 
  `adjust_city_name` string COMMENT '咨询师核算城市名称', 
  `adjust_city_seq` string COMMENT '带开城顺序的咨询师核算城市名称', 
  `mgr_adjust_city_id` bigint COMMENT '咨询师主管核算城市ID', 
  `mgr_adjust_city_name` string COMMENT '咨询师主管核算城市名称', 
  `mgr_adjust_city_seq` string COMMENT '带开城顺序的咨询师主管核算城市名称', 
  `subscribe_status` int COMMENT '退化 ：认购状态: 1：已认购  2：退认购', 
  `subscribe_type` int COMMENT '退化 ：认购类型: 1合作 4外联', 
  `orig_subsctibe_income` decimal(19,4) COMMENT '认购原合同预测总收入(佣金)', 
  `orig_adjust_subscribe_num` double COMMENT '原始核算认购量', 
  `adjust_subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '核算认购-含退、含外联收入(佣金)', 
  `adjust_subscribe_contains_ext_income` decimal(19,4) COMMENT '核算认购-不含退、含外联收入(佣金)', 
  `adjust_subscribe_contains_cancel_income` decimal(19,4) COMMENT '核算认购-含退、不含外联收入(佣金)', 
  `adjust_subscribe_coop_income` decimal(19,4) COMMENT '核算认购-合作、不含外联收入(佣金)', 
  `adjust_subscribe_contains_cancel_ext_num` double COMMENT '核算认购量-含退、含外联', 
  `adjust_subscribe_contains_ext_num` double COMMENT '核算认购量-不含退、含外联', 
  `adjust_subscribe_contains_cancel_num` double COMMENT '核算认购量-含退、不含外联', 
  `adjust_subscribe_coop_num` double COMMENT '核算认购量-合作、不含外联', 
  `subscribe_date` string COMMENT '认购日期:yyyy-MM-dd', 
  `create_date` string COMMENT '创建日期:yyyy-MM-dd', 
  `happen_date` string COMMENT '业务发生日期:yyyy-MM-dd', 
  `etl_time` string COMMENT 'ETL跑数时间', 
  `clue_id` bigint COMMENT '线索id')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_zxs_adjust_cust_subscribe_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'totalSize'='974', 
  'transient_lastDdlTime'='1590511204')
