CREATE EXTERNAL TABLE `julive_fact.hbase_global_id_exploded`(
  `id` string COMMENT 'from deserializer', 
  `global_id` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,F1:global_id', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='hbase_global_id_exploded', 
  'transient_lastDdlTime'='1574152458')
