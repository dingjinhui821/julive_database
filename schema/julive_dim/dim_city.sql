CREATE TABLE `julive_dim.dim_city`(
  `skey` string COMMENT '主键', 
  `city_id` bigint COMMENT '源系统城市ID', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '开城顺序', 
  `region` string COMMENT '所属大区', 
  `city_type` string COMMENT '城市类型 老城（含副区） 新城', 
  `city_type_first` string COMMENT '城市类型1 老城（含副区） 新城', 
  `city_type_second` string COMMENT '城市类型2 12老城 副区', 
  `city_type_third` string COMMENT '城市类型3 11老城 13新城 副区', 
  `mgr_city_seq` string COMMENT '主城 开城顺序城市名称', 
  `mgr_city` string COMMENT '主城', 
  `deputy_city` string COMMENT '主副城')
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
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_city'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'totalSize'='4479', 
  'transient_lastDdlTime'='1589439667')

