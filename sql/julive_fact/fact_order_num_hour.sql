set hive.execution.engine=spark;
set spark.app.name=fact_order_num_hour;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：小时级订单分布表
--   输 入 表：
--         ods.yw_order_1h                                    -- 订单小时同步
--         julive_dim.dim_city                                -- 城市维度
--         julive_dim.dim_channel_info                        -- 渠道维度



--   输 出 表：julive_fact.fact_order_num_hour
 
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/07/07 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：


INSERT OVERWRITE TABLE julive_fact.fact_order_num_hour 
SELECT
        order_id,
        city_laiyuan,
        city_intent,
        city_channel,
        channel_id,
        channel_name,
        create_day,
        create_hour,
        media_type,
        product_type,
        if(create_hour >=0 and create_hour <= 10,1,0) as 0_10_order_cnt,
        if(create_hour >10 and create_hour <= 12,1,0) as 10_12_order_cnt,
        if(create_hour >12 and create_hour <= 14,1,0) as 12_14_order_cnt,
        if(create_hour >14 and create_hour <= 16,1,0) as 14_16_order_cnt,
        if(create_hour >16 and create_hour <= 18,1,0) as 16_18_order_cnt,
        if(create_hour >18 and create_hour <= 21,1,0) as 18_21_order_cnt,
        if(create_hour >21,1,0) as 21_24_order_cnt,
        is_distribute,
	is_xs
FROM
    (SELECT
            order.id AS order_id,
            city1.city_name as city_laiyuan,
            city2.city_name as city_intent,
            dim_channel_info.city_name as city_channel,
            order.channel_id,
            dim_channel_info.channel_name,
            create_day,
            create_hour,
            dim_channel_info.media_name as media_type,
            dim_channel_info.module_name as product_type,
            order.is_distribute,
	    order.is_xs
    FROM
        (SELECT
                id,
                city_id,
                channel_id,
                from_unixtime(create_datetime,'yyyy-MM-dd') as create_day,
                from_unixtime(create_datetime,'HH') as create_hour,
                customer_intent_city,
                if(is_distribute = 1,1,0) as is_distribute,
		if(is_distribute != 16,1,0) as is_xs
        FROM ods.yw_order_1h
        )order
    LEFT JOIN julive_dim.dim_channel_info
    ON order.channel_id = dim_channel_info.channel_id
    
    LEFT JOIN julive_dim.dim_city city1
    ON order.city_id = city1.city_id
    
    LEFT JOIN julive_dim.dim_city city2
    ON order.customer_intent_city = city2.city_id
    )all
;

DROP VIEW IF EXISTS tmp_bi.order_num_1h;
CREATE VIEW IF NOT EXISTS tmp_bi.order_num_1h (
order_id                                                       COMMENT '订单ID',
city_laiyuan                                                   COMMENT '业务城市',
city_intent                                                    COMMENT '意向城市',
city_channel                                                   COMMENT '渠道城市',
channel_id                                                     COMMENT '渠道ID',
channel_name                                                   COMMENT '渠道名称',
create_day                                                     COMMENT '创建日期',
create_hour                                                    COMMENT '创建小时',
media_type                                                     COMMENT '媒体类型',
product_type                                                   COMMENT '产品类型',
0_10_order_cnt                                                 COMMENT '0-10点订单数',
10_12_order_cnt                                                COMMENT '10-12点订单数',
12_14_order_cnt                                                COMMENT '12-14点订单数',
14_16_order_cnt                                                COMMENT '14-16点订单数',
16_18_order_cnt                                                COMMENT '16-18点订单数',
18_21_order_cnt                                                COMMENT '18-21点订单数',
21_24_order_cnt                                                COMMENT '21-24点订单数',
is_distribute                                                  COMMENT '上户', 
is_xs                                                          COMMENT '线索'
)
COMMENT '小时级订单分布表'
AS SELECT
*
FROM julive_fact.fact_order_num_hour
;




