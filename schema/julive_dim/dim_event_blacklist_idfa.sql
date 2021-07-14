CREATE EXTERNAL TABLE `julive_dim.dim_event_blacklist_idfa`(
  `rowkey` string COMMENT 'from deserializer', 
  `idfa` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,\ncf:idfa\n', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dim_event_blacklist_idfa', 
  'transient_lastDdlTime'='1586232151')

