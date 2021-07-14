CREATE EXTERNAL TABLE `julive_app.project_search_tag_hot_whole_features`(
  `city_id` string, 
  `features` string, 
  `features_cnt` bigint)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/project_search_tag_hot_whole_features'
TBLPROPERTIES (
  'transient_lastDdlTime'='1570766513')

