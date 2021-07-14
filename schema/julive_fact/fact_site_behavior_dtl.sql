-- 2021-06-30 jiangshengyang@julive.com 修改
-- 1 由外部表改为内部表
-- 2 添加 referer 字段
drop table if exists julive_fact.fact_site_behavior_dtl;
create table if not exists julive_fact.fact_site_behavior_dtl (
  `clue_id` bigint COMMENT '线索ID',
  `clue_create_time` string COMMENT '线索创建时间',
  `is_distribute` int COMMENT '是否分配,及不分配的原因:1 上户',
  `channel_id` int COMMENT '渠道ID',
  `project_id` int COMMENT '楼盘ID',
  `channel_put` string COMMENT '推广渠道',
  `referer` string COMMENT '',
  `product_id` int COMMENT '产品ID',
  `sub_product_id` int COMMENT '产品子类ID',
  `op_type` int COMMENT '操作类型',
  `device_id` int COMMENT '设备类型,website表自带的,属于冗余字段,目前没有剔除口径',
  `media_id` int COMMENT '媒体ID',
  `media_name` string COMMENT '媒体名称',
  `creative_id` bigint COMMENT '创意ID',
  `keyword_id` bigint COMMENT '关键词ID',
  `from_source` int COMMENT '来源系统:1 website 2 appsite',
  `is_first_website` int COMMENT '是否线索网站首次channel_put留电记录',
  `create_time` string COMMENT '创建时间:yyyy-MM-dd HH:mm:ss',
  `update_time` string COMMENT '更新时间:yyyy-MM-dd HH:mm:ss',
  `etl_time` string COMMENT 'ETL跑数时间'
) COMMENT '网站行为数据事实表'
stored as parquet
;


