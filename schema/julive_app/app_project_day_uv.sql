CREATE TABLE `julive_app.app_project_day_uv`(
  `pdate` string, 
  `time_p` string, 
  `city_id` int, 
  `project_id` string, 
  `uv` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_project_day_uv'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='12', 
  'numRows'='12744979', 
  'rawDataSize'='493684453', 
  'totalSize'='506429432', 
  'transient_lastDdlTime'='1590525831')
 
