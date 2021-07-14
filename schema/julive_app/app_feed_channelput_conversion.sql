drop table if exists julive_app.app_feed_channelput_conversion;
create table if not exists julive_app.app_feed_channelput_conversion (
  `report_date` string COMMENT '报告日期',
  `customer_intent_city_name` string COMMENT '客户意向城市名称',
  `clue_id` bigint COMMENT '线索ID',
  `media_type` string COMMENT '媒体类型',
  `channel_name` string COMMENT '渠道名称',
  `channel_put` string COMMENT '投放渠道',
  `clue_num` bigint COMMENT '线索量',
  `distribute_num` bigint COMMENT '上户量',
  `see_num` bigint COMMENT '带看量',
  `subscribe_num` bigint COMMENT '认购量:含退、含外联',
  `subscribe_contains_cancel_ext_income` double COMMENT '认购-含退、含外联收入',
  `show_num` bigint COMMENT '展示',
  `click_num` bigint COMMENT '点击',
  `bill_cost` decimal(38,4) COMMENT '账面消耗',
  `cost` decimal(38,4) COMMENT '消耗量',
  `etl_time` string COMMENT 'ETL跑数时间'
) COMMENT 'feed渠道包转化'
stored as parquet;
