CREATE TABLE `julive_app.app_article_number`(
  `article_id` string COMMENT '文章id', 
  `click_num` int COMMENT '文章点击量')
COMMENT '文章点击量表'
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_article_number'
TBLPROPERTIES (
  'transient_lastDdlTime'='1575277315')
 