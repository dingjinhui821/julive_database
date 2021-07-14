CREATE TABLE `julive_app.app_short_url_info`(
  `media_id` int, 
  `account_id` int, 
  `plan_id` bigint, 
  `unit_id` bigint, 
  `pc_short_url` string, 
  `pc_channel_id` string, 
  `m_short_url` string, 
  `m_channel_id` string, 
  `plan_num` bigint, 
  `unit_num` bigint, 
  `keyword_num` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_short_url_info'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='1495546', 
  'rawDataSize'='181221043', 
  'totalSize'='182716589', 
  'transient_lastDdlTime'='1590539508')
 
