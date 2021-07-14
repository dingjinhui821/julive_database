CREATE TABLE `julive_app.project_rank_view_click`(
  `project_id` string, 
  `pdate` string, 
  `view_cnt` bigint, 
  `click_cnt` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/project_rank_view_click'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='2999057', 
  'rawDataSize'='64904500', 
  'totalSize'='67903557', 
  'transient_lastDdlTime'='1581684682')

