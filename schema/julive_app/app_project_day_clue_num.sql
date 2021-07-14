CREATE TABLE `julive_app.app_project_day_clue_num`(
  `order_time` string, 
  `city_id` int, 
  `project_id` string, 
  `sum_order` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_project_day_clue_num'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='4092835', 
  'rawDataSize'='121461197', 
  'totalSize'='125554032', 
  'transient_lastDdlTime'='1590527360')
 