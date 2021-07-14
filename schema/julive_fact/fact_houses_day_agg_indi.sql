CREATE TABLE `julive_fact.fact_houses_day_agg_indi`(
  `project_id` string COMMENT '楼盘id', 
  `project_name` string COMMENT '楼盘名称', 
  `project_city_id` string COMMENT '楼盘所在城市id', 
  `project_city_name` string COMMENT '楼盘所在城市名称', 
  `project_city_seq` string COMMENT '代开楼盘城市', 
  `clue_num` int COMMENT '楼盘日线索量', 
  `distribute_num` int COMMENT '楼盘日上户量', 
  `see_num` int COMMENT '楼盘日带看量', 
  `subscribe_contains_cancel_ext_num` int COMMENT '认购-含退含外联', 
  `subscribe_cancel_contains_ext_num` int COMMENT '退认购', 
  `sign_contains_cancel_ext_num` int COMMENT '签约', 
  `create_date` string COMMENT '时间', 
  `etl_time` string COMMENT '跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_houses_day_agg_indi'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='1253487', 
  'rawDataSize'='16295331', 
  'totalSize'='8743805', 
  'transient_lastDdlTime'='1590544644')
