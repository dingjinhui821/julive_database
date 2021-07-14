CREATE EXTERNAL TABLE `julive_dim.dim_wlmq_city`(
  `skey` string COMMENT '主键',
  from_source int COMMENT '数据来源: 2-乌鲁木齐虚拟项目数据 3-二手房中介', 
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
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_wlmq_city'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='4', 
  'rawDataSize'='246', 
  'totalSize'='250', 
  'transient_lastDdlTime'='1590510215')

