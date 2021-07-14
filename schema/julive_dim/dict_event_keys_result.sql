CREATE EXTERNAL TABLE `julive_dim.dict_event_keys_result`(
  `first_col_name` string COMMENT '一层key值', 
  `col_name` string COMMENT '字段名字', 
  `col_comment` string COMMENT '字段注释', 
  `map_col_name` string COMMENT '映射目标字段名称', 
  `event_cnt` int COMMENT '所属事件计数', 
  `is_delete` int COMMENT '是否逻辑删除')
COMMENT 'Hive解析埋点元数据字典表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期')
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
  'hdfs://optimuspro01:8020/dw/julive_dim/dict_event_keys_result'
TBLPROPERTIES (
  'transient_lastDdlTime'='1564055548')

