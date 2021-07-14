CREATE TABLE `julive_dim.dict_shence_cols_bak`(
  `col_name` string COMMENT '字段名字', 
  `col_comment` string COMMENT '字段注释')
COMMENT '神策埋点字段字典表备份'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dict_shence_cols_bak'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='840', 
  'rawDataSize'='19813', 
  'totalSize'='20653', 
  'transient_lastDdlTime'='1563874503')