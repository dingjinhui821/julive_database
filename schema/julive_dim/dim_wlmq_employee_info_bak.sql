CREATE TABLE `julive_dim.dim_wlmq_employee_info_bak`(
  `skey` string, 
  `emp_id` bigint, 
  `emp_name` string, 
  `job_number` string, 
  `card_no` string, 
  `sex` int, 
  `sex_tc` string, 
  `nation` string, 
  `domicile_address` string, 
  `marital_status` int, 
  `marital_status_tc` string, 
  `birthday` string, 
  `age` int, 
  `employment_form` int, 
  `employment_form_tc` string, 
  `political_outlook` string, 
  `graduation_school` string, 
  `school_attributes` string, 
  `graduation_date` string, 
  `major` string, 
  `first_work_date` string, 
  `high_major` int, 
  `high_major_tc` string, 
  `contract` string, 
  `ascription` string, 
  `orgi_entry_date` string, 
  `entry_date` string, 
  `full_date` string, 
  `full_type` int, 
  `full_type_tc` string, 
  `offjob_date` string, 
  `first_contract_date` string, 
  `end_contract_date` string, 
  `post_id` int, 
  `post_name` string, 
  `management_form` int, 
  `management_form_tc` string, 
  `dept_attr` int, 
  `dept_attr_tc` string, 
  `city_id` int, 
  `city_name` string, 
  `adjust_city_id` int, 
  `adjust_city_name` string, 
  `job_status` int, 
  `job_status_tc` string, 
  `direct_leader_id` int, 
  `direct_leader_name` string, 
  `indirect_leader_id` int, 
  `indirect_leader_name` string, 
  `dept_id` bigint, 
  `dept_name` string, 
  `dept_level` int, 
  `team_leader_id` int, 
  `team_leader_name` string, 
  `cate_id` int, 
  `cate_name` string, 
  `dept_type_id` int, 
  `dept_type_name` string, 
  `dept_level_leader` string, 
  `dept_id_first` int, 
  `dept_name_first` string, 
  `dept_leader_id_first` int, 
  `dept_leader_name_first` string, 
  `dept_id_second` int, 
  `dept_name_second` string, 
  `dept_leader_id_second` int, 
  `dept_leader_name_second` string, 
  `dept_id_third` int, 
  `dept_name_third` string, 
  `dept_leader_id_third` int, 
  `dept_leader_name_third` string, 
  `dept_id_fourth` int, 
  `dept_name_fourth` string, 
  `dept_leader_id_fourth` int, 
  `dept_leader_name_fourth` string, 
  `dept_id_fifth` int, 
  `dept_name_fifth` string, 
  `dept_leader_id_fifth` int, 
  `dept_leader_name_fifth` string, 
  `dept_id_sixth` int, 
  `dept_name_sixth` string, 
  `dept_leader_id_sixth` int, 
  `dept_leader_name_sixth` string, 
  `dept_id_seventh` int, 
  `dept_name_seventh` string, 
  `dept_leader_id_seventh` int, 
  `dept_leader_name_seventh` string, 
  `dept_id_eighth` int, 
  `dept_name_eighth` string, 
  `dept_leader_id_eighth` int, 
  `dept_leader_name_eighth` string, 
  `create_date` string, 
  `version` int, 
  `status` int, 
  `start_date` string, 
  `etl_time` string, 
  `end_date` string, 
  `p_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_wlmq_employee_info_bak'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='480', 
  'rawDataSize'='473207', 
  'totalSize'='473687', 
  'transient_lastDdlTime'='1589536147')

