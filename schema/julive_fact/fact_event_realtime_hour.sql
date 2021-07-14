CREATE EXTERNAL TABLE `julive_fact.fact_event_realtime_hour`(
  `id` string, 
  `product_id` string, 
  `event` string, 
  `frompage` string, 
  `topage` string, 
  `utm_source` string, 
  `channel` string)
PARTITIONED BY ( 
  `phour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_event_realtime_hour'
TBLPROPERTIES (
  'transient_lastDdlTime'='1576035961')
