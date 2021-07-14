CREATE TABLE `julive_app.app_clue_basinfo_indi_report`(
  `clue_id` bigint COMMENT '线索ID', 
  `channel_id` int COMMENT '对应渠道表的channel_id', 
  `channel_name` string COMMENT '渠道名称', 
  `media_id` int COMMENT '媒体类型ID', 
  `media_name` string COMMENT '媒体类型名称', 
  `module_id` int COMMENT '模块ID', 
  `module_name` string COMMENT '模块名称', 
  `device_id` int COMMENT '设备ID', 
  `device_name` string COMMENT '设备名称', 
  `city_id` int COMMENT '用户来源城市id', 
  `city_name` string COMMENT '用户来源城市名称', 
  `city_seq` string COMMENT '带开城顺序的用户来源城市名称', 
  `customer_intent_city_id` int COMMENT '客户意向城市ID', 
  `customer_intent_city_name` string COMMENT '客户意向城市名称', 
  `customer_intent_city_seq` string COMMENT '带开城顺序的客户意向城市名称', 
  `emp_id` int COMMENT '员工id', 
  `emp_name` string COMMENT '员工姓名', 
  `now_direct_leader_id` int COMMENT '当前员工直接上级id', 
  `now_direct_leader_name` string COMMENT '当前员工直接上级id', 
  `now_indirect_leader_id` int COMMENT '当前员工直接上级的上级员工ID', 
  `now_indirect_leader_name` string COMMENT '当前员工直接上级的上级员工姓名', 
  `direct_leader_id` int COMMENT '业务发生时员工直接上级id', 
  `direct_leader_name` string COMMENT '业务发生时员工直接上级id', 
  `indirect_leader_id` int COMMENT '业务发生时员工直接上级的上级员工ID', 
  `indirect_leader_name` string COMMENT '业务发生时员工直接上级的上级员工姓名', 
  `is_distribute` int COMMENT '是否分配，及不分配的原因：1、分配2、谈合作3、超区域4、不关注楼盘5、必须找售楼处6、关注二手房7、之前已上户8、不愿留电话9、电话无法接通99、其他', 
  `distribute_time` string COMMENT '分配时间:yyyy-MM-dd HH:mm:ss', 
  `distribute_date` string COMMENT '分配日期:yyyy-MM-dd', 
  `district_id` string COMMENT '关注区域id，逗号分隔', 
  `total_price_max` string COMMENT '最大总价', 
  `budget_range_grade` string COMMENT '预算区间：通过最大总价计算', 
  `interest_project` string COMMENT '客户感兴趣楼盘id，用逗号隔开', 
  `investment` string COMMENT '是否是投资', 
  `qualifications` string COMMENT '是否有资质', 
  `intent` int COMMENT '1.无意向 2.保留 3.有意向', 
  `intent_low_time` string COMMENT '变为无意向的时间:yyyy-MM-dd HH:mm:ss', 
  `purchase_purpose` int COMMENT '购房目的:1刚需，2改善，3投资，4自助+投资', 
  `purchase_purpose_tc` string COMMENT '购房目的:1刚需，2改善，3投资，4自助+投资', 
  `purchase_urgency` string COMMENT '购房紧迫度:1高，2中，3低', 
  `clue_num` int COMMENT '线索量', 
  `distribute_num` int COMMENT '上户量', 
  `see_num` int COMMENT '带看量', 
  `see_project_num` int COMMENT '带看楼盘量', 
  `subscribe_num` int COMMENT '认购量:含退、含外联', 
  `subscribe_coop_num` int COMMENT '净认购量:不含退、不含外联', 
  `sign_num` int COMMENT '签约量:含退、含外联', 
  `call_duration` int COMMENT '线索通话时长(秒)', 
  `call_num` int COMMENT '线索通话次数', 
  `clue_see_num` int COMMENT '产生带看的线索量', 
  `clue_subscribe_num` int COMMENT '产生认购的线索量', 
  `clue_sign_num` int COMMENT '产生签约的线索量', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '线索基础信息及相关指标基础报表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_clue_basinfo_indi_report'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='3177917', 
  'rawDataSize'='165251684', 
  'totalSize'='225419448', 
  'transient_lastDdlTime'='1590545497')
 
