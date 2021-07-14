CREATE EXTERNAL TABLE `julive_fact.global_id_exploded_hbase_2_hive`(
  `id` string, 
  `global_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/global_id_exploded_hbase_2_hive'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='75', 
  'numRows'='315805313', 
  'rawDataSize'='631610626', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"global_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'totalSize'='21257619663', 
  'transient_lastDdlTime'='1590512683')
