CREATE EXTERNAL TABLE `julive_app.tmp_project_order_cost`(
  `time_p` string, 
  `project_id` string, 
  `city_id` int, 
  `project_7cvr` double, 
  `city_7cvr` double, 
  `xs_cost` double, 
  `project_cost` double)
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
  'hdfs://optimuspro01:8020/dw/julive_app/tmp_project_order_cost'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='38647', 
  'rawDataSize'='4047423', 
  'totalSize'='4086070', 
  'transient_lastDdlTime'='1581417267')
