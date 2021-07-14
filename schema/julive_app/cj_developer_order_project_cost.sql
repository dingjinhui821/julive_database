CREATE TABLE `julive_app.cj_developer_order_project_cost`(
  `id` string COMMENT '订单id', 
  `order_time` string COMMENT '创建时间', 
  `city_id` bigint COMMENT '城市id', 
  `developer_id` bigint COMMENT '开发商id', 
  `project_id` bigint COMMENT '楼盘id', 
  `source` bigint COMMENT '来源', 
  `employee_id` bigint COMMENT '销售id', 
  `user_mobile` bigint COMMENT '用户电话', 
  `channel_id` bigint COMMENT '渠道id', 
  `user_id` bigint COMMENT '用户id', 
  `project_cost` bigint COMMENT '楼盘线索成本')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/cj_developer_order_project_cost'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='3746', 
  'rawDataSize'='254863', 
  'totalSize'='258609', 
  'transient_lastDdlTime'='1590470458')
 