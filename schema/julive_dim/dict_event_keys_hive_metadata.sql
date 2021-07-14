CREATE EXTERNAL TABLE `julive_dim.dict_event_keys_hive_metadata`(
  `product_id` string COMMENT '产品ID', 
  `event` string COMMENT '事件名称', 
  `first_col_name` string COMMENT '一层key值', 
  `col_name` string COMMENT '字段名字')
COMMENT 'Hive解析埋点元数据字典表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dict_event_keys_hive_metadata'
TBLPROPERTIES (
  'transient_lastDdlTime'='1564047999')

