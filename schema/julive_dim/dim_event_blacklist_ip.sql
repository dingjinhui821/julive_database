CREATE EXTERNAL TABLE `julive_dim.dim_event_blacklist_ip`(
  `rowkey` string COMMENT 'from deserializer', 
  `ip` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,\ncf:ip\n', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='dim_event_blacklist_ip', 
  'transient_lastDdlTime'='1586232169')

