CREATE EXTERNAL TABLE `julive_fact.global_id_hbase_2_hive`(
  `global_id` string, 
  `user_id` string, 
  `distinct_id` string, 
  `julive_id` string, 
  `cookie_id` string, 
  `visitor_id` string, 
  `comjia_unique_id` string, 
  `comjia_android_id` string, 
  `comjia_imei` string, 
  `idfa` string, 
  `idfv` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/global_id_hbase_2_hive'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='66', 
  'numRows'='171904195', 
  'rawDataSize'='1890946145', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"global_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"distinct_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"julive_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cookie_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"visitor_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"comjia_unique_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"comjia_android_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"comjia_imei\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"idfa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"idfv\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'totalSize'='17367784255', 
  'transient_lastDdlTime'='1590512600')
