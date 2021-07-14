CREATE EXTERNAL TABLE `julive_fact.hbase_global_id`(
  `global_id` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `distinct_id` string COMMENT 'from deserializer', 
  `julive_id` string COMMENT 'from deserializer', 
  `cookie_id` string COMMENT 'from deserializer', 
  `visitor_id` string COMMENT 'from deserializer', 
  `comjia_unique_id` string COMMENT 'from deserializer', 
  `comjia_android_id` string COMMENT 'from deserializer', 
  `comjia_imei` string COMMENT 'from deserializer', 
  `idfa` string COMMENT 'from deserializer', 
  `idfv` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,F1:user_id,F1:distinct_id,F1:julive_id,F1:cookie_id,F1:visitor_id,F1:comjia_unique_id,F1:comjia_android_id,F1:comjia_imei,F1:idfa,F1:idfv', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='hbase_global_id', 
  'transient_lastDdlTime'='1574151209')
