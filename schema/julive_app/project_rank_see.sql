CREATE TABLE `julive_app.project_rank_see`(
  `project_id` bigint, 
  `pdate` string, 
  `see_cnt` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/project_rank_see'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='195443', 
  'rawDataSize'='3702942', 
  'totalSize'='3898385', 
  'transient_lastDdlTime'='1581684986')
 
