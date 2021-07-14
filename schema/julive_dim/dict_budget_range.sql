CREATE EXTERNAL TABLE `julive_dim.dict_budget_range`(
  `id` int COMMENT '', 
  `city_id` int COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `grade_id` int COMMENT '预算等级id', 
  `grade_name` string COMMENT '预算等级', 
  `low_budget` bigint COMMENT '预算下限', 
  `high_budget` bigint COMMENT '预算上限', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '更新时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dict_budget_range'
TBLPROPERTIES (
  'transient_lastDdlTime'='1571295853')
