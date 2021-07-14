drop table if exists julive_fact.fact_subscribe_base_dtl;
CREATE EXTERNAL TABLE  julive_fact.fact_subscribe_base_dtl (
   subscribe_id  bigint COMMENT '认购id', 
   clue_id  bigint COMMENT '线索id', 
   see_id  bigint COMMENT '带看id', 
   channel_id  bigint COMMENT '渠道ID', 
   deal_id  bigint COMMENT '成交id', 
   project_id  bigint COMMENT '楼盘id', 
   project_name  string COMMENT '楼盘名称', 
   emp_id  bigint COMMENT '员工id', 
   emp_name  string COMMENT '员工名称', 
   user_id  bigint COMMENT '用户id', 
   user_name  string COMMENT '用户名称',
   region                                 string         comment '大区',
mgr_city_seq                           string         comment '主城 开城顺序城市名称',
mgr_city                               string         comment '主城',
 
   city_id  bigint COMMENT '线索城市id', 
   city_name  string COMMENT '线索城市名称', 
   city_seq  string COMMENT '带开城顺序城市名称', 
   customer_intent_city_id  int COMMENT '客户意向城市ID', 
   customer_intent_city_name  string COMMENT '客户意向城市名称', 
   customer_intent_city_seq  string COMMENT '带开城顺序客户意向城市名称', 
   project_city_id  int COMMENT '认购楼盘所在城市ID', 
   project_city_name  string COMMENT '认购楼盘所在城市名称', 
   project_city_seq  string COMMENT '带开城顺序认购楼盘所在城市名称', 
   subscribe_status  int COMMENT '退化 ：认购状态: 1：已认购  2：退认购', 
   subscribe_type  int COMMENT '退化 ：认购类型: 1合作 4外联', 
   is_first_see  int COMMENT '是否首付访认购:1是 0否', 
   is_first_see_project  int COMMENT '是否订单-楼盘首付访认购:1是 0否', 
   orig_deal_amt  decimal(19,4) COMMENT '原成交金额', 
   orig_subsctibe_income  decimal(19,4) COMMENT '原合同预测总收入', 
   orig_subscribe_num  int COMMENT '认购量--未指定合作和外联', 
   orig_subscribe_contains_cancel_num  int COMMENT '含退认购--未指定合作和外联', 
   subscribe_contains_cancel_ext_amt  decimal(19,4) COMMENT '认购-含退、含外联GMV', 
   subscribe_contains_cancel_ext_income  decimal(19,4) COMMENT '认购-含退、含外联收入', 
   subscribe_contains_ext_amt  decimal(19,4) COMMENT '认购-含外联GMV', 
   subscribe_contains_ext_income  decimal(19,4) COMMENT '认购-含外联收入', 
   subscribe_coop_amt  decimal(19,4) COMMENT '认购-不含外联GMV', 
   subscribe_coop_income  decimal(19,4) COMMENT '认购-不含外联收入', 
   subscribe_cancel_contains_ext_amt  decimal(19,4) COMMENT '退认购金额-含外联', 
   subscribe_cancel_contains_ext_income  decimal(19,4) COMMENT '退认购佣金-含外联', 
   subscribe_cancel_coop_amt  decimal(19,4) COMMENT '退认购金额-不含外联', 
   subscribe_contains_cancel_ext_num  int COMMENT '认购量-含退、含外联', 
   subscribe_contains_cancel_num  int COMMENT '认购量-含退、不含外联', 
   subscribe_coop_num  int COMMENT '认购量-不含外联', 
   subscribe_contains_ext_num  int COMMENT '认购量-含外联', 
   subscribe_cancel_coop_num  int COMMENT '退认购量-不含外联', 
   subscribe_cancel_contains_ext_num  int COMMENT '退认购量-含外联', 
   subscribe_time  string COMMENT '认购时间', 
   back_time  string COMMENT '退认购单时间', 
  from_source  int    COMMENT '1-自营 2-乌鲁木齐 3-二手房中介' ,
   create_time  string COMMENT '创建时间', 
   etl_time  string COMMENT 'ETL跑数时间')
COMMENT '认购明细事实表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_subscribe_base_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='32914', 
  'rawDataSize'='1546958', 
  'totalSize'='6163461', 
  'transient_lastDdlTime'='1591050059')
;
