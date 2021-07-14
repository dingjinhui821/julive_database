CREATE EXTERNAL TABLE `julive_fact.fact_subscribe_no_sign_indi`(
  `deal_id` bigint COMMENT '成交id', 
  `subscribe_id` bigint COMMENT '认购id', 
  `grass_sign_id` bigint COMMENT '草签id', 
  `clue_id` bigint COMMENT '线索id', 
  `project_city_id` bigint COMMENT '楼盘地理城市id', 
  `project_city_name` string COMMENT '楼盘地理城市名称', 
  `project_city_seq` string COMMENT '楼盘地理城市名称带开城顺序', 
  `mgr_city_name` string COMMENT '楼盘地理城市主城名称', 
  `mgr_city_seq` string COMMENT '楼盘地理城市主城名称带开城顺序', 
  `project_id` bigint COMMENT '楼盘id', 
  `project_name` string COMMENT '楼盘名称', 
  `project_type` string COMMENT '楼盘类型', 
  `emp_id` bigint COMMENT '认购单员工id', 
  `emp_name` string COMMENT '认购单员工姓名', 
  `clue_emp_id` bigint COMMENT '订单所属员工id', 
  `clue_emp_name` string COMMENT '订单所属员工姓名', 
  `direct_leader_id` int COMMENT '订单所属咨询师业务发生时主管ID', 
  `direct_leader_name` string COMMENT '订单所属咨询师业务发生时主管名称', 
  `indirect_leader_id` int COMMENT '订单所属咨询师业务发生时经理ID', 
  `indirect_leader_name` string COMMENT '订单所属咨询师业务发生时经理名称', 
  `now_direct_leader_id` int COMMENT '订单所属咨询师当前主管ID', 
  `now_direct_leader_name` string COMMENT '订单所属咨询师当前主管名称', 
  `now_indirect_leader_id` int COMMENT '订单所属咨询师当前经理ID', 
  `now_indirect_leader_name` string COMMENT '订单所属咨询师当前经理名称', 
  `subscribe_status` int COMMENT '退化 ：认购状态: 1：已认购  2：退认购', 
  `subscribe_type` int COMMENT '退化 ：认购类型: 1合作 4外联', 
  `grass_sign_status` int COMMENT '3:已草签 4:退草签 ', 
  `is_have_risk` int COMMENT '客户签约是否有风险:1是，2否', 
  `receive_amt` decimal(19,4) COMMENT '应收金额', 
  `actual_amt` decimal(19,4) COMMENT '实收金额', 
  `subscribe_date` string COMMENT '认购时间', 
  `actual_date` string COMMENT '实收时间', 
  `grass_sign_date` string COMMENT '草签时间', 
  `back_date` string COMMENT '退认购时间', 
  `plan_sign_date` string COMMENT '预计签约时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '认购未签约全量表'
PARTITIONED BY ( 
  `pdate` string COMMENT '分区日期:记录日期 yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_subscribe_no_sign_indi'
TBLPROPERTIES (
  'transient_lastDdlTime'='1577518818')
