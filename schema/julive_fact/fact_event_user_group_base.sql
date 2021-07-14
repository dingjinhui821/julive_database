CREATE EXTERNAL TABLE `julive_fact.fact_event_user_group_base`(
  `skey` string COMMENT '记录唯一标识', 
  `session_id` string COMMENT '默认规则切割session标识:pc-5分钟 m-1分钟 app(视频)-3分钟 小程序-1分钟 其他-不切割session', 
  `prev_event_elapse` double COMMENT '与上一条事件时间间隔,单位(秒)', 
  `next_event_elapse` double COMMENT '与下一条事件时间间隔,单位(秒)', 
  `user_access_seq_asc_today` int COMMENT '当天内用户访问行为序列-正序', 
  `user_access_seq_desc_today` int COMMENT '当天内用户访问行为序列-倒序', 
  `global_id` string COMMENT '全局唯一ID', 
  `user_id` bigint COMMENT '神策USER_ID', 
  `julive_id` bigint COMMENT '居理用户ID', 
  `comjia_unique_id` string COMMENT '用户ID', 
  `cookie_id` string COMMENT 'COOKIE ID', 
  `idfa` string COMMENT 'IOS设备名', 
  `idfv` string COMMENT 'IOS设备编号', 
  `p_ip` string COMMENT '用户使用设备的 IP', 
  `comjia_imei` string COMMENT 'imei-安卓设备标识:coalesce(comjia_imei,imei)', 
  `fl_project_id` string COMMENT '项目ID', 
  `fl_project_name` string COMMENT '项目名称', 
  `product_id` string COMMENT '产品ID', 
  `product_name` string COMMENT '产品名称', 
  `event` string COMMENT '事件名称', 
  `app_version` string COMMENT 'APP版本号:短', 
  `p_app_version` string COMMENT 'APP版本号', 
  `abtest_name` string COMMENT 'AB测试名称', 
  `abtest_value` string COMMENT 'AB测试值', 
  `select_city` string COMMENT '选择的城市ID', 
  `frompage` string COMMENT '当前页面', 
  `topage` string COMMENT '到达页面', 
  `frommodule` string COMMENT '当前模块', 
  `tomodule` string COMMENT '到达模块', 
  `leave_phone_state` string COMMENT '留电状态', 
  `user_agent` string COMMENT 'UA', 
  `order_id` bigint COMMENT '订单ID', 
  `is_new_order` string COMMENT '是否为新订单', 
  `current_url` string COMMENT '当前URL', 
  `to_url` string COMMENT '目标URL', 
  `referer` string COMMENT '来源url', 
  `op_type` string COMMENT '留电位置', 
  `source` string COMMENT '联系途径', 
  `channel_id` string COMMENT '渠道ID', 
  `channel_put` string COMMENT '推广渠道', 
  `login_state` string COMMENT '登录状态', 
  `p_utm_source` string COMMENT '首次广告系列来源', 
  `button_title` string COMMENT '按钮名称', 
  `operation_type` string COMMENT '运营位类型', 
  `operation_position` string COMMENT '运营位置名字', 
  `banner_id` string COMMENT 'BANNER位置', 
  `tab_id` string COMMENT '标签ID', 
  `p_is_first_day` string COMMENT '是否首次访问:1是 0否', 
  `abtest_is_first_day` string COMMENT '是否首次参与实验:1是 0否', 
  `create_time` string COMMENT '创建时间:yyyy-MM-dd HH:mm:ss', 
  `pplatform` string COMMENT '产品端分区:101 pc | 102 m | 103 app | 104 miniprogram | 999 others', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '埋点线上用户分析基表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_event_user_group_base'
TBLPROPERTIES (
  'transient_lastDdlTime'='1571484484')
