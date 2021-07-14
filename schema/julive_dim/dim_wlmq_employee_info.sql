CREATE EXTERNAL TABLE `julive_dim.dim_wlmq_employee_info`(
  `skey` string COMMENT '部门表代理键:主键', 
  `emp_id` bigint COMMENT '员工id', 
  `emp_name` string COMMENT '员工姓名', 
  `from_source` int COMMENT '数据来源: 1-居理数据 2-乌鲁木齐虚拟项目数据', 
  `job_number` string COMMENT '员工工号', 
  `card_no` string COMMENT '身份证号', 
  `sex` int COMMENT '性别 1男 2女', 
  `sex_tc` string COMMENT '性别 1男 2女', 
  `nation` string COMMENT '民族', 
  `domicile_address` string COMMENT '户籍', 
  `marital_status` int COMMENT '婚姻状况 1已婚  2未婚', 
  `marital_status_tc` string COMMENT '婚姻状况 1已婚  2未婚', 
  `birthday` string COMMENT '生日', 
  `age` int COMMENT '年龄', 
  `employment_form` int COMMENT '聘用形式', 
  `employment_form_tc` string COMMENT '聘用形式', 
  `political_outlook` string COMMENT '政治面貌', 
  `graduation_school` string COMMENT '毕业学校', 
  `school_attributes` string COMMENT '学校类型', 
  `graduation_date` string COMMENT '毕业日期', 
  `major` string COMMENT '专业', 
  `first_work_date` string COMMENT '正式参加工作日期', 
  `high_major` int COMMENT '学历 1初中 2高中 3大专 4本科 5硕士 6博士 7其他', 
  `high_major_tc` string COMMENT '学历 1初中 2高中 3大专 4本科 5硕士 6博士 7其他', 
  `contract` string COMMENT '合同归属', 
  `ascription` string COMMENT '社保、公积金归属', 
  `orgi_entry_date` string COMMENT '人力表-入职日期', 
  `entry_date` string COMMENT '入职日期', 
  `full_date` string COMMENT '转正日期', 
  `full_type` int COMMENT '转正状态 1未转正 2已转正', 
  `full_type_tc` string COMMENT '转正状态 1未转正 2已转正', 
  `offjob_date` string COMMENT '离职时间', 
  `first_contract_date` string COMMENT '合同起始日', 
  `end_contract_date` string COMMENT '合同到期日', 
  `post_id` int COMMENT '岗位id', 
  `post_name` string COMMENT '职位', 
  `management_form` int COMMENT '管理形式 1总公司 2分公司', 
  `management_form_tc` string COMMENT '管理形式 1总公司 2分公司', 
  `dept_attr` int COMMENT '部门属性 1支撑部门 2职能部门 3业务部门', 
  `dept_attr_tc` string COMMENT '部门属性 1支撑部门 2职能部门 3业务部门', 
  `city_id` int COMMENT '部门所在城市', 
  `city_name` string COMMENT '部门所在城市', 
  `adjust_city_id` int COMMENT '员工核算城市ID', 
  `adjust_city_name` string COMMENT '员工核算城市名称', 
  `job_status` int COMMENT '在职情况  0离职 1在职', 
  `job_status_tc` string COMMENT '在职情况  0离职 1在职', 
  `direct_leader_id` int COMMENT '员工直接上级id', 
  `direct_leader_name` string COMMENT '员工直接上级id', 
  `indirect_leader_id` int COMMENT '员工直接上级的上级员工ID', 
  `indirect_leader_name` string COMMENT '员工直接上级的上级员工姓名', 
  `dept_id` bigint COMMENT '部门ID', 
  `dept_name` string COMMENT '部门名称', 
  `dept_level` int COMMENT '部门层级', 
  `team_leader_id` int COMMENT '领导ID', 
  `team_leader_name` string COMMENT '领导名称', 
  `cate_id` int COMMENT '部分分类ID，1大区 2城市公司 3中心 4部门 5组', 
  `cate_name` string COMMENT '部分分类名称，1大区 2城市公司 3中心 4部门 5组', 
  `dept_type_id` int COMMENT '部门标志位ID 1咨询组，2咨询部，3渠道部，4售前咨询、0其他', 
  `dept_type_name` string COMMENT '部门标志位名称 1咨询组，2咨询部，3渠道部，4售前咨询、0其他', 
  `dept_level_leader` string COMMENT '各部门领导', 
  `dept_id_first` int COMMENT '一级部门ID', 
  `dept_name_first` string COMMENT '一级部门名称', 
  `dept_leader_id_first` int COMMENT '一级部门leader ID', 
  `dept_leader_name_first` string COMMENT '一级部门leader Name', 
  `dept_id_second` int COMMENT '二级部门ID', 
  `dept_name_second` string COMMENT '二级部门名称', 
  `dept_leader_id_second` int COMMENT '二级部门leader ID', 
  `dept_leader_name_second` string COMMENT '二级部门leader Name', 
  `dept_id_third` int COMMENT '三级部门ID', 
  `dept_name_third` string COMMENT '三级部门名称', 
  `dept_leader_id_third` int COMMENT '三级部门leader ID', 
  `dept_leader_name_third` string COMMENT '三级部门leader Name', 
  `dept_id_fourth` int COMMENT '四级部门ID', 
  `dept_name_fourth` string COMMENT '四级部门名称', 
  `dept_leader_id_fourth` int COMMENT '四级部门leader ID', 
  `dept_leader_name_fourth` string COMMENT '四级部门leader Name', 
  `dept_id_fifth` int COMMENT '五级部门ID', 
  `dept_name_fifth` string COMMENT '五级部门名称', 
  `dept_leader_id_fifth` int COMMENT '五级部门leader ID', 
  `dept_leader_name_fifth` string COMMENT '五级部门leader Name', 
  `dept_id_sixth` int COMMENT '六级部门ID', 
  `dept_name_sixth` string COMMENT '六级部门名称', 
  `dept_leader_id_sixth` int COMMENT '六级部门leader ID', 
  `dept_leader_name_sixth` string COMMENT '六级部门leader Name', 
  `dept_id_seventh` int COMMENT '七级部门ID', 
  `dept_name_seventh` string COMMENT '七级部门名称', 
  `dept_leader_id_seventh` int COMMENT '七级部门leader ID', 
  `dept_leader_name_seventh` string COMMENT '七级部门leader Name', 
  `dept_id_eighth` int COMMENT '八级部门ID', 
  `dept_name_eighth` string COMMENT '八级部门名称', 
  `dept_leader_id_eighth` int COMMENT '八级部门leader ID', 
  `dept_leader_name_eighth` string COMMENT '八级部门leader Name', 
  `create_date` string COMMENT '创建日期：yyyy-MM-dd', 
  `version` int COMMENT '记录版本号', 
  `status` int COMMENT '当前状态:1 当前数据 0 归档数据', 
  `start_date` string COMMENT '记录生效日期：yyyy-MM-dd', 
  `etl_time` string COMMENT 'ETL跑数日期', 
  `end_date` string COMMENT '分区日期:记录失效日期 yyyy-MM-dd')
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_wlmq_employee_info'
TBLPROPERTIES (
  'transient_lastDdlTime'='1589767368')
