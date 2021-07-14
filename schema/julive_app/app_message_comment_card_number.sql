CREATE TABLE `julive_app.app_message_comment_card_number`(
  `comment_card_id` string COMMENT '信息流', 
  `click_num` int COMMENT '信息流量')
COMMENT '信息流量表'
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_message_comment_card_number'
TBLPROPERTIES (
  'transient_lastDdlTime'='1575344365')
 