CREATE TABLE `julive_app.project_rank_subscribe`(
  `project_id` bigint, 
  `pdate` string, 
  `subscribe_cnt` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/project_rank_subscribe'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='78736', 
  'rawDataSize'='1493747', 
  'totalSize'='1572483', 
  'transient_lastDdlTime'='1581685479')
