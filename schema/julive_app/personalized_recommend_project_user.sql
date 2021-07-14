CREATE EXTERNAL TABLE `julive_app.personalized_recommend_project_user`(
  `id` string COMMENT 'from deserializer', 
  `project_id` string COMMENT 'from deserializer', 
  `comjia_unique_id_list` string COMMENT 'from deserializer', 
  `city_id` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key,info:project_id,info:comjia_unique_id_list,info:city_id', 
  'serialization.format'='1')
TBLPROPERTIES (
  'hbase.table.name'='personalized_recommend_project_user', 
  'transient_lastDdlTime'='1569737389')
 