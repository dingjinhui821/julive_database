CREATE TABLE `julive_app.tmp_cjw_project_uv`(
  `pdate` string, 
  `city_id` int, 
  `time_p` string, 
  `project_id` string, 
  `uv` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/tmp_cjw_project_uv'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='3810642', 
  'rawDataSize'='148064527', 
  'totalSize'='151875169', 
  'transient_lastDdlTime'='1590520148')
