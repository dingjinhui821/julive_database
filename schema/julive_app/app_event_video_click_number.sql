CREATE TABLE `julive_app.app_event_video_click_number`(
  `video_id` string, 
  `total` string)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_event_video_click_number'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574241435')
