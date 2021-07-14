CREATE EXTERNAL TABLE `julive_app.app_owl_consultant_performance`(
  `event` string, 
  `track_id` string, 
  `frompage` string, 
  `topage` string, 
  `login_employee_id` string, 
  `view_time` string, 
  `create_timestamp` bigint)
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
  'hdfs://optimuspro01:8020/dw/julive_app/app_owl_consultant_performance'
TBLPROPERTIES (
  'transient_lastDdlTime'='1573115401')
 