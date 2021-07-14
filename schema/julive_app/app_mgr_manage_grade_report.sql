CREATE TABLE `julive_app.app_mgr_manage_grade_report`(
  `mgr_id` bigint COMMENT '主管id', 
  `mgr_name` string COMMENT '主管姓名', 
  `adjust_city_id` bigint COMMENT '核算城市id', 
  `adjust_city_name` string COMMENT '核算城市名称', 
  `adjust_city_seq` string COMMENT '核算城市名称加开城顺序', 
  `loss_man_power` decimal(19,4) COMMENT '主管流失人力数', 
  `loss_man_power_city` decimal(19,4) COMMENT '城市流失平均人力数', 
  `loss_standard_scores` decimal(19,4) COMMENT '主管流失标准分', 
  `person_coefficient_scores` decimal(19,4) COMMENT '人头系数分', 
  `mgr_high_educated_rate` decimal(19,4) COMMENT '主管高学历占比', 
  `city_high_educated_rate` decimal(19,4) COMMENT '城市高学历占比', 
  `high_educated_scores` decimal(19,4) COMMENT '主管高学历分', 
  `hatch_scores` decimal(19,4) COMMENT '主管孵化分=主管孵化人力数', 
  `high_rank_promote_scores` decimal(19,4) COMMENT '主管高职级晋升分', 
  `yearmonth` string COMMENT '月份', 
  `etl_time` string COMMENT '数据刷新时间')
COMMENT '主管管理分数'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_mgr_manage_grade_report'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='2424', 
  'rawDataSize'='38784', 
  'totalSize'='215455', 
  'transient_lastDdlTime'='1590527924')
 
