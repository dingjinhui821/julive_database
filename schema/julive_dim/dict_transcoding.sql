CREATE EXTERNAL TABLE `julive_dim.dict_transcoding`(
  `id` int COMMENT '', 
  `code_type` string COMMENT '码类型', 
  `code_desc` string COMMENT '码中文描述', 
  `from_tab_col` string COMMENT '来源业务表字段,完整格式,可适当选择描述:[db.table.column]', 
  `src_code_value` string COMMENT '源码值', 
  `src_code_name` string COMMENT '源描述', 
  `tgt_code_value` string COMMENT '目标码值', 
  `tgt_code_name` string COMMENT '目标描述', 
  `is_general` int COMMENT '是否为通用码:1 是 0 否', 
  `is_repeat` int COMMENT '是否已重复:1是0否', 
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
  'hdfs://optimuspro01:8020/dw/ods/dict_transcoding'
TBLPROPERTIES (
  'transient_lastDdlTime'='1564468136')

