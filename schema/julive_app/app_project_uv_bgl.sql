CREATE TABLE `julive_app.app_project_uv_bgl`(
  `pdate` string COMMENT '创建时间', 
  `time_p` string COMMENT '浏览时间', 
  `project_id` bigint COMMENT '楼盘id', 
  `product_id` bigint COMMENT '设备id', 
  `city_id` bigint COMMENT '城市id', 
  `name` string COMMENT '楼盘名称', 
  `bgl` bigint COMMENT '曝光量', 
  `uv` bigint COMMENT 'UV')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_project_uv_bgl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='12', 
  'numRows'='14033529', 
  'rawDataSize'='895353084', 
  'totalSize'='909386613', 
  'transient_lastDdlTime'='1590529108')
 
