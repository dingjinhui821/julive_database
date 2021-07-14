CREATE TABLE `julive_app.cj_developer_order`(
  `id` int, 
  `city_id` int, 
  `developer_id` int, 
  `project_id` bigint, 
  `project_name` string, 
  `saler_name` string, 
  `saler_phone` string, 
  `channel_id` int, 
  `user_phone` string, 
  `creator` int, 
  `updator` int, 
  `create_datetime` int, 
  `update_datetime` int, 
  `op_type` int, 
  `user_id` string, 
  `district_ids` string, 
  `min_price` int, 
  `max_price` int, 
  `etl_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/cj_developer_order'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='8732', 
  'rawDataSize'='1592936', 
  'totalSize'='1601668', 
  'transient_lastDdlTime'='1590549172')
 