CREATE TABLE `julive_fact.fact_see_comment`(
  `clue_id` bigint COMMENT '订单id', 
  `see_id` bigint COMMENT '带看表id', 
  `see_emp_id` bigint COMMENT '带看咨询师id', 
  `see_emp_name` string COMMENT '带看咨询师姓名', 
  `emp_leader_id` bigint COMMENT '咨询师主管id', 
  `emp_leader_name` string COMMENT '咨询师主管姓名', 
  `city_id` bigint COMMENT '原城市ID', 
  `city_name` string COMMENT '原城市名称', 
  `city_seq` string COMMENT '带开城顺序的原城市名称', 
  `project_city_id` int COMMENT '楼盘所在城市ID', 
  `project_city_name` string COMMENT '楼盘所在城市名称', 
  `project_city_seq` string COMMENT '带开城顺序的楼盘所在城市名称', 
  `probability` int COMMENT '推荐侃家的概率:0-100', 
  `final_grade` int COMMENT '员工评价打分:0非常不满意 1不满意 3一般 4满意 5非常满意', 
  `is_txt_comment` int COMMENT '是否存在文字评论', 
  `txt_comment_num` int COMMENT '文字评价数量', 
  `comment_num` int COMMENT '评价次数', 
  `visit_time` string COMMENT '回访时间', 
  `create_time` string COMMENT '创建时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '带看评价事实表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_see_comment'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='221511', 
  'rawDataSize'='4430220', 
  'totalSize'='16439251', 
  'transient_lastDdlTime'='1590544347')
