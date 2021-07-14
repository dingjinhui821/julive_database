CREATE TABLE `julive_app.app_house_uv_test02`(
  `project_id` string, 
  `house_id` string, 
  `uv_one_day` string, 
  `uv_seven_day` string, 
  `browse_time` string, 
  `order_time` string, 
  `etl_time` string, 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_house_uv_test02'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='46023', 
  'rawDataSize'='3072602', 
  'totalSize'='3118625', 
  'transient_lastDdlTime'='1590547807')

