CREATE TABLE `julive_app.app_project_brow_num`(
  `project_id` bigint, 
  `user_brows_num` int)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_project_brow_num'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"project_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"user_brows_num\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"pdate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.partCol.0'='pdate', 
  'transient_lastDdlTime'='1583482035')
 
