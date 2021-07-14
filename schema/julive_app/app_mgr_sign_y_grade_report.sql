CREATE TABLE `julive_app.app_mgr_sign_y_grade_report`(
  `emp_mgr_id` bigint COMMENT '主管id', 
  `emp_mgr_name` string COMMENT '主管姓名', 
  `adjust_mgr_city_id` bigint COMMENT '业绩核算主城城市id', 
  `adjust_mgr_city_name` string COMMENT '业绩核算主城城市名称', 
  `adjust_mgr_city_seq` string COMMENT '业绩核算主城城市名称加开城顺序', 
  `city_id` bigint COMMENT '主管组织城市城市id', 
  `city_name` string COMMENT '主管组织城市城市名称', 
  `city_seq` string COMMENT '主管组织城市城市名称加开城顺序', 
  `adjust_sign_contains_cancel_ext_income` decimal(19,4) COMMENT '主管核算签约应收 含退含外联', 
  `adjust_subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '主管核算认购应收 含退含外联', 
  `emp_num_stand` decimal(19,4) COMMENT '主管人头数', 
  `city_y_value` decimal(19,4) COMMENT '城市y值', 
  `mgr_y_scores` decimal(19,4) COMMENT '主管签约y值得分', 
  `mgr_sub_y_scores` decimal(19,4) COMMENT '主管认购y值得分', 
  `yearmonth` string COMMENT '月份', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '主管签约y值分值'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_mgr_sign_y_grade_report'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='2961', 
  'rawDataSize'='47376', 
  'totalSize'='174065', 
  'transient_lastDdlTime'='1590546198')
 