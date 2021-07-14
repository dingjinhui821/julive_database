CREATE TABLE `julive_app.app_house_uv`(
  `project_id` string COMMENT '楼盘id', 
  `house_id` string COMMENT '户型id', 
  `uv_one_day` string COMMENT '1天uv数量', 
  `uv_seven_day` string COMMENT '7天uv数量', 
  `browse_time` string COMMENT '最近浏览时间', 
  `order_time` string COMMENT '最近订单时间', 
  `etl_time` string COMMENT 'etl跑数时间')
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_house_uv'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"project_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8id\"}},{\"name\":\"house_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6237\u578Bid\"}},{\"name\":\"uv_one_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"1\u5929uv\u6570\u91CF\"}},{\"name\":\"uv_seven_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"7\u5929uv\u6570\u91CF\"}},{\"name\":\"browse_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u8FD1\u6D4F\u89C8\u65F6\u95F4\"}},{\"name\":\"order_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u8FD1\u8BA2\u5355\u65F6\u95F4\"}},{\"name\":\"etl_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"etl\u8DD1\u6570\u65F6\u95F4\"}},{\"name\":\"pdate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.partCol.0'='pdate', 
  'transient_lastDdlTime'='1587388372')
