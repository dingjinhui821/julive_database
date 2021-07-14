CREATE EXTERNAL TABLE `julive_app.project_list_score_result`(
  `city_id` int, 
  `project_id` string, 
  `score` double, 
  `score_hezuo_wailian` double, 
  `score_res` bigint, 
  `rank` int)
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
  'hdfs://optimuspro01:8020/dw/julive_app/project_list_score_result'
TBLPROPERTIES (
  'transient_lastDdlTime'='1569499355')
 