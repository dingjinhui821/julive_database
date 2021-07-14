CREATE TABLE `julive_app.cjw_cost_project_t_cvr`(
  `time_p` string, 
  `project_id` string, 
  `city_id` int, 
  `project_7cvr` double, 
  `city_7cvr` double, 
  `xs_cost` double, 
  `project_cost` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/cjw_cost_project_t_cvr'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='199704', 
  'rawDataSize'='20610230', 
  'totalSize'='20809934', 
  'transient_lastDdlTime'='1590467979')
 