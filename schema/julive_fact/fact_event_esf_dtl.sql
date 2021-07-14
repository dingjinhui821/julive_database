CREATE EXTERNAL TABLE `julive_fact.fact_event_esf_dtl`(
  `skey` string COMMENT '记录唯一标识', 
  `session_id` string COMMENT '默认规则切割session标识:pc-5分钟 m-1分钟 app(视频)-3分钟 小程序-1分钟 其他-不切割session', 
  `prev_event_elapse` double COMMENT '与上一条事件时间间隔,单位(秒)', 
  `next_event_elapse` double COMMENT '与下一条事件时间间隔,单位(秒)', 
  `user_access_seq_asc_today` int COMMENT '当天内用户访问行为序列-row_number正序', 
  `user_access_seq_desc_today` int COMMENT '当天内用户访问行为序列-row_number倒序', 
  `user_access_seq_asc_today_dr` int COMMENT '当天内用户访问行为序列-dense_rank正序', 
  `user_access_seq_desc_today_dr` int COMMENT '当天内用户访问行为序列-dense_rank倒序', 
  `global_id` string COMMENT '全局用户标识', 
  `distinct_id` string COMMENT '神策自动生成的用户标识', 
  `map_id` string COMMENT '映射id通常和DISTINCT_ID相同', 
  `user_id` string COMMENT '神策USER_ID', 
  `julive_id` string COMMENT '居理用户ID', 
  `comjia_unique_id` string COMMENT '用户ID', 
  `cookie_id` string COMMENT 'COOKIE ID', 
  `open_id` string COMMENT '公众号用户唯一标识', 
  `product_id` string COMMENT '产品ID', 
  `product_name` string COMMENT '产品名称', 
  `track_id` string COMMENT '埋点ID(对应CMS中)', 
  `event` string COMMENT '事件名称', 
  `create_time` string COMMENT '事件创建时间(time):yyyy-MM-dd HH:mm:ss', 
  `create_timestamp` string COMMENT '事件创建原始时间戳', 
  `recv_time` string COMMENT 'nginx接收时间:yyyy-MM-dd HH:mm:ss', 
  `fl_project_id` string COMMENT '项目ID', 
  `fl_project_name` string COMMENT '项目名称', 
  `app_id` string COMMENT '设备端', 
  `p_ip` string COMMENT '用户使用设备的 IP', 
  `idfa` string COMMENT 'IOS设备名', 
  `idfv` string COMMENT 'IOS设备编号', 
  `channel_id` string COMMENT '渠道ID', 
  `login_employee_id` string COMMENT '员工工号', 
  `project_id` string COMMENT '进入页面前的楼盘ID', 
  `comjia_android_id` string COMMENT '安卓id设备标识', 
  `comjia_device_id` string COMMENT '设备id', 
  `comjia_imei` string COMMENT 'imei-安卓设备标识:coalesce(comjia_imei,imei)', 
  `visitor_id` string COMMENT '用户id(pc/m=cookie_id/distinct_id app=加密后install_id 小程序=open_id)', 
  `order_id` string COMMENT '订单ID', 
  `adviser_id` string COMMENT '咨询师ID', 
  `fromitemindex` string COMMENT '位置编号', 
  `fromitem` string COMMENT '按钮', 
  `frompage` string COMMENT '当前页面', 
  `topage` string COMMENT '到达页面', 
  `frommodule` string COMMENT '当前模块', 
  `tomodule` string COMMENT '到达模块', 
  `p_is_first_day` string COMMENT '是否首日访问', 
  `p_is_first_time` string COMMENT '是否首次访问', 
  `app_version` string COMMENT 'APP版本号:短', 
  `p_app_version` string COMMENT 'APP版本号', 
  `select_city` string COMMENT '选择的城市ID', 
  `lat` string COMMENT '经度', 
  `lng` string COMMENT '维度', 
  `current_url` string COMMENT '落地页的URL', 
  `to_url` string COMMENT '跳转链接', 
  `referer` string COMMENT '来源url', 
  `user_agent` string COMMENT 'UA', 
  `p_utm_source` string COMMENT '首次广告系列来源', 
  `op_type` string COMMENT '留电位置', 
  `track_type` string COMMENT '埋点类型(计算属性)：1 新埋点 2 神策埋点 3 旧埋点', 
  `is_new_order` string COMMENT '是否为新订单', 
  `channel_put` string COMMENT '推广渠道', 
  `login_state` string COMMENT '登录状态', 
  `button_title` string COMMENT '按钮名称', 
  `banner_id` string COMMENT 'BANNER位置', 
  `tab_id` string COMMENT '标签ID', 
  `leave_phone_state` string COMMENT '留电状态', 
  `source` string COMMENT '联系途径', 
  `operation_type` string COMMENT '运营位类型', 
  `operation_position` string COMMENT '运营位置名字', 
  `abtest_name` string COMMENT 'AB测试名称', 
  `abtest_value` string COMMENT 'AB测试值', 
  `lib` string COMMENT '埋点数据Json', 
  `properties` string COMMENT '埋点数据Json', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '埋点数据明细事实基表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期', 
  `pplatform` string COMMENT '产品端分区:101 pc | 102 m | 103 app | 104 miniprogram | 999 other')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_event_esf_dtl'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='2', 
  'spark.sql.sources.schema.numParts'='2', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"skey\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BB0\u5F55\u552F\u4E00\u6807\u8BC6\"}},{\"name\":\"session_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u9ED8\u8BA4\u89C4\u5219\u5207\u5272session\u6807\u8BC6:pc-5\u5206\u949F m-1\u5206\u949F app(\u89C6\u9891)-3\u5206\u949F \u5C0F\u7A0B\u5E8F-1\u5206\u949F \u5176\u4ED6-\u4E0D\u5207\u5272session\"}},{\"name\":\"prev_event_elapse\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E0E\u4E0A\u4E00\u6761\u4E8B\u4EF6\u65F6\u95F4\u95F4\u9694,\u5355\u4F4D(\u79D2)\"}},{\"name\":\"next_event_elapse\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E0E\u4E0B\u4E00\u6761\u4E8B\u4EF6\u65F6\u95F4\u95F4\u9694,\u5355\u4F4D(\u79D2)\"}},{\"name\":\"user_access_seq_asc_today\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u5929\u5185\u7528\u6237\u8BBF\u95EE\u884C\u4E3A\u5E8F\u5217-row_number\u6B63\u5E8F\"}},{\"name\":\"user_access_seq_desc_today\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u5929\u5185\u7528\u6237\u8BBF\u95EE\u884C\u4E3A\u5E8F\u5217-row_number\u5012\u5E8F\"}},{\"name\":\"user_access_seq_asc_today_dr\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u5929\u5185\u7528\u6237\u8BBF\u95EE\u884C\u4E3A\u5E8F\u5217-dense_rank\u6B63\u5E8F\"}},{\"name\":\"user_access_seq_desc_today_dr\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u5929\u5185\u7528\u6237\u8BBF\u95EE\u884C\u4E3A\u5E8F\u5217-dense_rank\u5012\u5E8F\"}},{\"name\":\"global_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5168\u5C40\u7528\u6237\u6807\u8BC6\"}},{\"name\":\"distinct_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u795E\u7B56\u81EA\u52A8\u751F\u6210\u7684\u7528\u6237\u6807\u8BC6\"}},{\"name\":\"map_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6620\u5C04id\u901A\u5E38\u548CDISTINCT_ID\u76F8\u540C\"}},{\"name\":\"user_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u795E\u7B56USER_ID\"}},{\"name\":\"julive_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5C45\u7406\u7528\u6237ID\"}},{\"name\":\"comjia_unique_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7528\u6237ID\"}},{\"name\":\"cookie_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"COOKIE ID\"}},{\"name\":\"open_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u516C\u4F17\u53F7\u7528\u6237\u552F\u4E00\u6807\u8BC6\"}},{\"name\":\"product_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4EA7\u54C1ID\"}},{\"name\":\"product_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4EA7\u54C1\u540D\u79F0\"}},{\"name\":\"track_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CB\u70B9ID(\u5BF9\u5E94CMS\u4E2D)\"}},{\"name\":\"event\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E8B\u4EF6\u540D\u79F0\"}},{\"name\":\"create_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E8B\u4EF6\u521B\u5EFA\u65F6\u95F4(time):yyyy-MM-dd HH:mm:ss\"}},{\"name\":\"create_timestamp\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E8B\u4EF6\u521B\u5EFA\u539F\u59CB\u65F6\u95F4\u6233\"}},{\"name\":\"recv_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"nginx\u63A5\u6536\u65F6\u95F4:yyyy-MM-dd HH:mm:ss\"}},{\"name\":\"fl_project_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u9879\u76EEID\"}},{\"name\":\"fl_project_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u9879\u76EE\u540D\u79F0\"}},{\"name\":\"app_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BBE\u5907\u7AEF\"}},{\"name\":\"p_ip\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7528\u6237\u4F7F\u7528\u8BBE\u5907\u7684 IP\"}},{\"name\":\"idfa\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"IOS\u8BBE\u5907\u540D\"}},{\"name\":\"idfv\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"IOS\u8BBE\u5907\u7F16\u53F7\"}},{\"name\":\"channel_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6E20\u9053ID\"}},{\"name\":\"login_employee_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5458\u5DE5\u5DE5\u53F7\"}},{\"name\":\"project_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8FDB\u5165\u9875\u9762\u524D\u7684\u697C\u76D8ID\"}},{\"name\":\"comjia_android_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5B89\u5353id\u8BBE\u5907\u6807\u8BC6\"}},{\"name\":\"comjia_device_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BBE\u5907id\"}},{\"name\":\"comjia_imei\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"imei-\u5B89\u5353\u8BBE\u5907\u6807\u8BC6:coalesce(comjia_imei,imei)\"}},{\"name\":\"visitor_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7528\u6237id(pc/m=cookie_id/distinct_id app=\u52A0\u5BC6\u540Einstall_id \u5C0F\u7A0B\u5E8F=open_id)\"}},{\"name\":\"order_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BA2\u5355ID\"}},{\"name\":\"adviser_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u54A8\u8BE2\u5E08ID\"}},{\"name\":\"fromitemindex\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4F4D\u7F6E\u7F16\u53F7\"}},{\"name\":\"fromitem\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6309\u94AE\"}},{\"name\":\"frompage\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u524D\u9875\u9762\"}},{\"name\":\"topage\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comme', 
  'spark.sql.sources.schema.part.1'='nt\":\"\u5230\u8FBE\u9875\u9762\"}},{\"name\":\"frommodule\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u524D\u6A21\u5757\"}},{\"name\":\"tomodule\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5230\u8FBE\u6A21\u5757\"}},{\"name\":\"p_is_first_day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u9996\u65E5\u8BBF\u95EE\"}},{\"name\":\"p_is_first_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u9996\u6B21\u8BBF\u95EE\"}},{\"name\":\"app_version\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"APP\u7248\u672C\u53F7:\u77ED\"}},{\"name\":\"p_app_version\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"APP\u7248\u672C\u53F7\"}},{\"name\":\"select_city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u9009\u62E9\u7684\u57CE\u5E02ID\"}},{\"name\":\"lat\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7ECF\u5EA6\"}},{\"name\":\"lng\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7EF4\u5EA6\"}},{\"name\":\"current_url\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u843D\u5730\u9875\u7684URL\"}},{\"name\":\"to_url\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8DF3\u8F6C\u94FE\u63A5\"}},{\"name\":\"referer\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6765\u6E90url\"}},{\"name\":\"user_agent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"UA\"}},{\"name\":\"p_utm_source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u9996\u6B21\u5E7F\u544A\u7CFB\u5217\u6765\u6E90\"}},{\"name\":\"op_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7559\u7535\u4F4D\u7F6E\"}},{\"name\":\"track_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CB\u70B9\u7C7B\u578B(\u8BA1\u7B97\u5C5E\u6027)\uFF1A1 \u65B0\u57CB\u70B9 2 \u795E\u7B56\u57CB\u70B9 3 \u65E7\u57CB\u70B9\"}},{\"name\":\"is_new_order\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u4E3A\u65B0\u8BA2\u5355\"}},{\"name\":\"channel_put\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u63A8\u5E7F\u6E20\u9053\"}},{\"name\":\"login_state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u767B\u5F55\u72B6\u6001\"}},{\"name\":\"button_title\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6309\u94AE\u540D\u79F0\"}},{\"name\":\"banner_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"BANNER\u4F4D\u7F6E\"}},{\"name\":\"tab_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6807\u7B7EID\"}},{\"name\":\"leave_phone_state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7559\u7535\u72B6\u6001\"}},{\"name\":\"source\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8054\u7CFB\u9014\u5F84\"}},{\"name\":\"operation_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8FD0\u8425\u4F4D\u7C7B\u578B\"}},{\"name\":\"operation_position\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8FD0\u8425\u4F4D\u7F6E\u540D\u5B57\"}},{\"name\":\"abtest_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"AB\u6D4B\u8BD5\u540D\u79F0\"}},{\"name\":\"abtest_value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"AB\u6D4B\u8BD5\u503C\"}},{\"name\":\"lib\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CB\u70B9\u6570\u636EJson\"}},{\"name\":\"properties\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CB\u70B9\u6570\u636EJson\"}},{\"name\":\"etl_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"ETL\u8DD1\u6570\u65F6\u95F4\"}},{\"name\":\"pdate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6570\u636E\u65E5\u671F\"}},{\"name\":\"pplatform\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4EA7\u54C1\u7AEF\u5206\u533A:101 pc | 102 m | 103 app | 104 miniprogram | 999 other\"}}]}', 
  'spark.sql.sources.schema.partCol.0'='pdate', 
  'spark.sql.sources.schema.partCol.1'='pplatform', 
  'transient_lastDdlTime'='1575936096')
