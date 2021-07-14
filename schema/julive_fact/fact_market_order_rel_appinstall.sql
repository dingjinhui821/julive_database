drop table if exists julive_fact.fact_market_order_rel_appinstall;
CREATE EXTERNAL TABLE julive_fact.fact_market_order_rel_appinstall(
order_id                                            int           COMMENT '订单id', 
create_order_date                                   string        COMMENT '订单创建日期：yyyy-MM-dd',
create_order_time                                   string        COMMENT '订单创建时间戳：yyyy-MM-dd HH:mm:ss',
channel_id                                          int           COMMENT '渠道id',
channel_name                                        string        COMMENT '渠道名称',
user_id                                             int           COMMENT '用户id',
global_id                                           string        comment '客户端全局id',
utm_source                                          string        comment 'utm_source',
select_city                                         string        comment '城市',
`$city`                                             string        comment '城市ip解析',
product_id	                                        string        comment '手机类型101安卓 102苹果', 
channel                                             string        comment '渠道名', 
install_date_time                                   string        COMMENT '实际消耗',
aid                                                 string        COMMENT '创意ID',
cid                                                 string        COMMENT '计划id', 
etl_time                                            string        COMMENT 'ETL跑数时间')
COMMENT '订单关联AppInstall表'
PARTITIONED BY (`pdate` string)
stored as parquet;
