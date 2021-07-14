CREATE TABLE `julive_app.app_client_brand_info`(
  `manufacturer` string COMMENT '品牌厂商信息', 
  `model` string COMMENT '终端设备信息', 
  `user_agent` string COMMENT 'user_agent中截取信息')
COMMENT '埋点客户端品牌信息底表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_client_brand_info'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"manufacturer\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u54C1\u724C\u5382\u5546\u4FE1\u606F\"}},{\"name\":\"model\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7EC8\u7AEF\u8BBE\u5907\u4FE1\u606F\"}},{\"name\":\"user_agent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"user_agent\u4E2D\u622A\u53D6\u4FE1\u606F\"}},{\"name\":\"pdate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6570\u636E\u65E5\u671F\"}}]}', 
  'spark.sql.sources.schema.partCol.0'='pdate', 
  'transient_lastDdlTime'='1577443360')
 