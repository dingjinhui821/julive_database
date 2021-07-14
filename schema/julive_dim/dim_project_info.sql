drop table if exists julive_dim.dim_project_info;
CREATE EXTERNAL TABLE `julive_dim.dim_project_info`(
  `skey` string COMMENT '楼盘表代理键:主键', 
  `project_id` bigint COMMENT '楼盘id', 
  `project_name` string COMMENT '楼盘名称', 
  `project_num` string COMMENT '楼盘编号', 
  `summary` string COMMENT '楼盘概述', 
  `far` string COMMENT '容积率', 
  `heating` string COMMENT '供暖方式', 
  `car_space` string COMMENT '车位比', 
  `greening` string COMMENT '绿化率', 
  `developer` string COMMENT '开发商', 
  `address` string COMMENT '位置描述', 
  `ring_road` double COMMENT '环线', 
  `near_distance` string COMMENT '近环距离', 
  `far_distance` string COMMENT '远环距离', 
  `coordinate` string COMMENT '楼盘百度坐标', 
  `ambitus_text` string COMMENT '周边描述', 
  `city_id` bigint COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `district_id` bigint COMMENT '区域id', 
  `district_name` string COMMENT '区域名称', 
  `project_type` bigint COMMENT '业态', 
  project_type_desc string comment '业态名称',
  `acreage_min` double COMMENT '最小面积', 
  `acreage_max` double COMMENT '最大面积', 
  `price_min` double COMMENT '最小总价', 
  `price_max` double COMMENT '最大总价', 
  `near_subway` int COMMENT '临地铁', 
  `project_status` int COMMENT '状态，1: 未售  2:在售  3:售罄 4:待售', 
  `is_cooperate` int COMMENT '是否是合作楼盘，1:是  2:否', 
  `is_discount` int COMMENT '是否折扣:1是，2否根据pay_info来判断，有值为1，无值为2', 
  `updator` bigint COMMENT '楼盘维护咨询师', 
  `is_outreach` int COMMENT '是否外联:0不是，1是', 
  `lat_lng` string COMMENT '售楼处经纬度', 
  `lng` string COMMENT '楼盘经度', 
  `lat` string COMMENT '楼盘纬度', 
  `trade_area` bigint COMMENT '区域板块id', 
  `is_loft` int COMMENT '是否loft:1是，2否', 
  `ec_flow` int COMMENT '是否刷电商,1是,2否', 
  `is_show` bigint COMMENT '楼盘是否显示 1显示 2隐藏', 
  `create_date` bigint COMMENT '创建日期:yyyy-MM-dd', 
  `version` int COMMENT '记录版本号', 
  `status` int COMMENT '当前状态:1 当前数据 0 归档数据', 
  `start_date` string COMMENT '记录生效日期：yyyy-MM-dd', 
  `etl_time` string COMMENT 'ETL跑数日期')
COMMENT '楼盘维度表'
PARTITIONED BY ( 
  `end_date` string COMMENT '分区日期:记录失效日期 yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_project_info'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"skey\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u8868\u4EE3\u7406\u952E:\u4E3B\u952E\"}},{\"name\":\"project_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8id\"}},{\"name\":\"project_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u540D\u79F0\"}},{\"name\":\"project_num\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u7F16\u53F7\"}},{\"name\":\"summary\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u6982\u8FF0\"}},{\"name\":\"far\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5BB9\u79EF\u7387\"}},{\"name\":\"heating\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4F9B\u6696\u65B9\u5F0F\"}},{\"name\":\"car_space\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8F66\u4F4D\u6BD4\"}},{\"name\":\"greening\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u7EFF\u5316\u7387\"}},{\"name\":\"developer\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F00\u53D1\u5546\"}},{\"name\":\"address\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4F4D\u7F6E\u63CF\u8FF0\"}},{\"name\":\"ring_road\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u73AF\u7EBF\"}},{\"name\":\"near_distance\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8FD1\u73AF\u8DDD\u79BB\"}},{\"name\":\"far_distance\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8FDC\u73AF\u8DDD\u79BB\"}},{\"name\":\"coordinate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u767E\u5EA6\u5750\u6807\"}},{\"name\":\"ambitus_text\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5468\u8FB9\u63CF\u8FF0\"}},{\"name\":\"city_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CE\u5E02id\"}},{\"name\":\"city_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u57CE\u5E02\u540D\u79F0\"}},{\"name\":\"district_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u533A\u57DFid\"}},{\"name\":\"district_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u533A\u57DF\u540D\u79F0\"}},{\"name\":\"project_type\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E1A\u6001\"}},{\"name\":\"acreage_min\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u5C0F\u9762\u79EF\"}},{\"name\":\"acreage_max\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u5927\u9762\u79EF\"}},{\"name\":\"price_min\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u5C0F\u603B\u4EF7\"}},{\"name\":\"price_max\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"comment\":\"\u6700\u5927\u603B\u4EF7\"}},{\"name\":\"near_subway\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u4E34\u5730\u94C1\"}},{\"name\":\"project_status\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u72B6\u6001\uFF0C1: \u672A\u552E  2:\u5728\u552E  3:\u552E\u7F44 4:\u5F85\u552E\"}},{\"name\":\"is_cooperate\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u662F\u5408\u4F5C\u697C\u76D8\uFF0C1:\u662F  2:\u5426\"}},{\"name\":\"is_discount\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u6298\u6263:1\u662F\uFF0C2\u5426\u6839\u636Epay_info\u6765\u5224\u65AD\uFF0C\u6709\u503C\u4E3A1\uFF0C\u65E0\u503C\u4E3A2\"}},{\"name\":\"updator\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u7EF4\u62A4\u54A8\u8BE2\u5E08\"}},{\"name\":\"is_outreach\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u5916\u8054:0\u4E0D\u662F\uFF0C1\u662F\"}},{\"name\":\"lat_lng\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u552E\u697C\u5904\u7ECF\u7EAC\u5EA6\"}},{\"name\":\"lng\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u7ECF\u5EA6\"}},{\"name\":\"lat\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u7EAC\u5EA6\"}},{\"name\":\"trade_area\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u533A\u57DF\u677F\u5757id\"}},{\"name\":\"is_loft\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426loft:1\u662F\uFF0C2\u5426\"}},{\"name\":\"ec_flow\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u662F\u5426\u5237\u7535\u5546,1\u662F,2\u5426\"}},{\"name\":\"is_show\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u697C\u76D8\u662F\u5426\u663E\u793A 1\u663E\u793A 2\u9690\u85CF\"}},{\"name\":\"create_date\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\u521B\u5EFA\u65E5\u671F:yyyy-MM-dd\"}},{\"name\":\"version\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BB0\u5F55\u7248\u672C\u53F7\"}},{\"name\":\"status\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5F53\u524D\u72B6\u6001:1 \u5F53\u524D\u6570\u636E 0 \u5F52\u6863\u6570\u636E\"}},{\"name\":\"start_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u8BB0\u5F55\u751F\u6548\u65E5\u671F\uFF1Ayyyy-MM-dd\"}},{\"name\":\"etl_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"ETL\u8DD1\u6570\u65E5\u671F\"}},{\"name\":\"end_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"\u5206\u533A\u65E5\u671F:\u8BB0\u5F55\u5931\u6548\u65E5\u671F yyyy-MM-dd\"}}]}', 
  'spark.sql.sources.schema.partCol.0'='end_date', 
  'transient_lastDdlTime'='1574310051')
