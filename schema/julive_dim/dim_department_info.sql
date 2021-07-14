CREATE EXTERNAL TABLE `julive_dim.dim_department_info`(
  `skey` string COMMENT '部门表代理键:主键', 
  `dept_id` bigint COMMENT '部门ID', 
  `dept_name` string COMMENT '部门名称', 
  `dept_level` int COMMENT '部门层级', 
  `team_leader_id` int COMMENT '领导ID', 
  `team_leader_name` string COMMENT '领导名称', 
  `city_id` int COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `cate_id` int COMMENT '部分分类ID，1大区 2城市公司 3中心 4部门 5组', 
  `cate_name` string COMMENT '部分分类名称，1大区 2城市公司 3中心 4部门 5组', 
  `dept_type_id` int COMMENT '部门标志位ID 1咨询组，2咨询部，3渠道部，4售前咨询、0其他', 
  `dept_type_name` string COMMENT '部门标志位名称 1咨询组，2咨询部，3渠道部，4售前咨询、0其他', 
  `dept_attr` int COMMENT '部门属性：1 支撑部门 2业务部门 3 职能部门', 
  `dept_level_path` string COMMENT '分类路径', 
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
  `is_archived` string COMMENT '当前是否属于归档部门信息：1是 0否', 
  `version` int COMMENT '记录版本号', 
  `status` int COMMENT '当前状态:1 当前数据 0 归档数据', 
  `start_date` string COMMENT '记录生效日期：yyyy-MM-dd', 
  `etl_time` string COMMENT 'ETL跑数日期')
COMMENT '部门维度表'
PARTITIONED BY ( 
  `end_date` string COMMENT '分区日期:记录失效日期 yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_department_info'
TBLPROPERTIES (
  'transient_lastDdlTime'='1567498496')

