CREATE TABLE `julive_fact.fact_return_visit`(
  `clue_id` bigint COMMENT '线索ID', 
  `emp_id` bigint COMMENT '咨询师id', 
  `emp_name` string COMMENT '咨询师姓名', 
  `emp_leader_id` bigint COMMENT '咨询师主管id', 
  `emp_leader_name` string COMMENT '咨询师主管姓名', 
  `city_id` int COMMENT '原城市id', 
  `city_name` string COMMENT '原城市名称', 
  `city_seq` string COMMENT '带开城顺序的原城市名称', 
  `customer_intent_city_id` int COMMENT '客户意向城市ID', 
  `customer_intent_city_name` string COMMENT '客户意向城市名称', 
  `customer_intent_city_seq` string COMMENT '带开城顺序的客户意向城市名称', 
  `is_init_eval` int COMMENT '是否客户主动评价：1是 0否', 
  `continue_server` int COMMENT '是否仍需要侃家服务：0未知，1不需要，2继续服务', 
  `final_grade` int COMMENT '0非常不满意 1不满意 3一般 4满意 5非常满意', 
  `is_txt_comment` int COMMENT '是否存在文字评论', 
  `txt_comment_num` int COMMENT '文字评价数量', 
  `comment_num` int COMMENT '评价次数', 
  `visit_time` string COMMENT '回访时间', 
  `create_time` string COMMENT '创建时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '无意向回访记录事实表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_return_visit'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='1860156', 
  'rawDataSize'='37203120', 
  'totalSize'='121584032', 
  'transient_lastDdlTime'='1590544387')
