CREATE TABLE `julive_app.app_event_only_report_indi`(
  `report_id` string COMMENT '埋点报告ID', 
  `total` string COMMENT '指标')
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期', 
  `ptype` string COMMENT 'type_tag类型')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_event_only_report_indi'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='2', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"report_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CB\u70B9\u62A5\u544AID\"}},{\"name\":\"total\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6307\u6807\"}},{\"name\":\"pdate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6570\u636E\u65E5\u671F\"}},{\"name\":\"ptype\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"type_tag\u7C7B\u578B\"}}]}', 
  'spark.sql.sources.schema.partCol.0'='pdate', 
  'spark.sql.sources.schema.partCol.1'='ptype', 
  'transient_lastDdlTime'='1574669454')
