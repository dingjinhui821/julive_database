CREATE TABLE `julive_app.app_warning_monitor_indi`(
  `city_id` int COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `week_id` int COMMENT '周标识', 
  `week_range` string COMMENT '周', 
  `col1` string COMMENT '预留字段', 
  `col2` string COMMENT '预留字段', 
  `col3` string COMMENT '预留字段', 
  `col4` string COMMENT '预留字段', 
  `col5` string COMMENT '预留字段', 
  `current_value` string COMMENT '指标值', 
  `up_value` string COMMENT '监控上限值', 
  `down_value` string COMMENT '监控下限值')
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期', 
  `pindi` string COMMENT '监控指标')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_warning_monitor_indi'
TBLPROPERTIES (
  'transient_lastDdlTime'='1589255736')
 
