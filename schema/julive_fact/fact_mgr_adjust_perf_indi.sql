CREATE TABLE `julive_fact.fact_mgr_adjust_perf_indi`(
  `emp_mgr_id` bigint COMMENT '咨询师主管ID', 
  `emp_mgr_name` string COMMENT '咨询师主管名称', 
  `mgr_adjust_city_id` bigint COMMENT '核算城市ID', 
  `mgr_adjust_city_name` string COMMENT '核算城市名称', 
  `mgr_adjust_city_seq` string COMMENT '核算城市名称', 
  `zxs_mgr_adjust_city_id` bigint COMMENT '咨询师维度核算城市ID', 
  `zxs_mgr_adjust_city_name` string COMMENT '咨询师维度核算城市名称', 
  `zxs_mgr_adjust_city_seq` string COMMENT '咨询师维度核算城市名称', 
  `happen_date` string COMMENT '业务发生日期:yyyy-MM-dd', 
  `entry_date` string COMMENT '入职日期', 
  `full_date` string COMMENT '转正日期', 
  `full_type` int COMMENT '转正状态', 
  `offjob_date` string COMMENT '离职日期', 
  `post_id` int COMMENT '岗位ID', 
  `post_name` string COMMENT '岗位名称', 
  `dept_id` int COMMENT '部门ID', 
  `dept_name` string COMMENT '部门名称', 
  `direct_leader_id` int COMMENT '业务发生时主管ID', 
  `direct_leader_name` string COMMENT '业务发生时主管名称', 
  `indirect_leader_id` int COMMENT '业务发生时经理ID', 
  `indirect_leader_name` string COMMENT '业务发生时经理名称', 
  `now_direct_leader_id` int COMMENT '当前主管ID', 
  `now_direct_leader_name` string COMMENT '当前主管名称', 
  `now_indirect_leader_id` int COMMENT '当前经理ID', 
  `now_indirect_leader_name` string COMMENT '当前经理名称', 
  `promotion_date` string COMMENT '晋升主管日期:yyyy-MM-dd', 
  `adjust_distribute_num` float COMMENT '核算上户量', 
  `first_adjust_distribute_num` float COMMENT '首次核算上户量', 
  `adjust_see_num` float COMMENT '核算带看量', 
  `adjust_subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '核算认购-含退、含外联收入(佣金)', 
  `adjust_subscribe_contains_ext_income` decimal(19,4) COMMENT '核算认购-不含退、含外联收入(佣金)', 
  `adjust_subscribe_contains_cancel_income` decimal(19,4) COMMENT '核算认购-含退、不含外联收入(佣金)', 
  `adjust_subscribe_coop_income` decimal(19,4) COMMENT '核算认购-合作、不含外联收入(佣金)', 
  `adjust_subscribe_contains_cancel_ext_num` float COMMENT '核算认购量-含退、含外联', 
  `adjust_subscribe_contains_ext_num` float COMMENT '核算认购量-不含退、含外联', 
  `adjust_subscribe_contains_cancel_num` float COMMENT '核算认购量-含退、不含外联', 
  `adjust_subscribe_coop_num` float COMMENT '核算认购量-合作、不含外联', 
  `adjust_subscribe_cancel_contains_ext_income` decimal(19,4) COMMENT '核算退认购佣金-含外联(佣金)', 
  `adjust_subscribe_cancel_contains_ext_num` float COMMENT '核算退认购量-含外联', 
  `adjust_sign_contains_cancel_ext_income` decimal(19,4) COMMENT '核算签约-含退、含外联收入(佣金)', 
  `adjust_sign_contains_ext_income` decimal(19,4) COMMENT '核算签约-不含退、含外联收入(佣金)', 
  `adjust_sign_contains_cancel_income` decimal(19,4) COMMENT '核算签约-含退、不含外联收入(佣金)', 
  `adjust_sign_coop_income` decimal(19,4) COMMENT '核算签约-合作、不含外联收入(佣金)', 
  `adjust_sign_contains_cancel_ext_num` float COMMENT '核算签约量-含退、含外联', 
  `adjust_sign_contains_ext_num` float COMMENT '核算签约量-不含退、含外联', 
  `adjust_sign_contains_cancel_num` float COMMENT '核算签约量-含退、不含外联', 
  `adjust_sign_coop_num` float COMMENT '核算签约量-合作、不含外联', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '咨询师核算性能指标表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_mgr_adjust_perf_indi'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='1026081', 
  'rawDataSize'='49251888', 
  'totalSize'='87578704', 
  'transient_lastDdlTime'='1590546057')
