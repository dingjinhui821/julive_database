CREATE EXTERNAL TABLE `julive_app.personalized_recommend_project_score_one_by_one`(
  `id` string COMMENT 'from deserializer', 
  `score` string COMMENT 'from deserializer', 
  `city_id` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,info:score,info:city_id', 
  'serialization.format'='1')
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'hbase.table.name'='personalized_recommend_project_score_one_by_one', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1569294432')
 
