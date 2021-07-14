--  功能描述：feed渠道包转化（线索，上户，带看，认购，展，点，消）
--  输 入 表：
--        803 julive_fact.fact_site_behavior_dtl                            -- 留电信息表（包含website和appsite）
--       2974 julive_dim.dim_clue_ext_base                                  -- 扩展线索维度表
--       2975 julive_topic.topic_dws_order_city_base                        -- 订单城市业务指标表
--       2987 julive_fact.fact_dsp_creative_report_dtl                      -- 创意报告明细事实表
--       2902 julive_dim.dim_dsp_account                                    -- DSP账户纬度表
--       2930 julive_dim.dim_dsp_account_rebate                             -- 渠道返点维度表
--       手动维护不需要加依赖 julive_dim.dim_city                               -- 城市维度表
--
--  输 出 表：julive_app.app_feed_channelput_conversion                      -- feed渠道包转化
--
--
--  创 建 者： 蒋胜洋  13971279523  jiangshengyang@julive.com
--  创建日期： 2021/06/25 17:16
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--  

set hive.execution.engine=spark;
set spark.app.name=app_feed_channelput_conversion;
set mapred.job.name=app_feed_channelput_conversion;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=24;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;

drop table if exists tmp_etl.tmp_fact_site_behavior_dtl_01;

create table if not exists tmp_etl.tmp_fact_site_behavior_dtl_01 (
  `clue_id` bigint COMMENT '线索ID',
  `channel_id_1` string COMMENT 'url解析渠道ID',
  `channel_put_1` string COMMENT 'url解析投放渠道',
  `ad_id` string COMMENT 'url解析广告id',
  `creative_id` string COMMENT 'url解析创意id',
  `channel_id` string COMMENT '渠道ID',
  `channel_put` string COMMENT '投放渠道',
  `from_source` int COMMENT '1 website,2 appsite',
  `leavephone_time` string COMMENT '留电时间'
) COMMENT '留电临时表'
stored as parquet;


-- 从留电表url中解析channel_id和channel_put
insert overwrite table tmp_etl.tmp_fact_site_behavior_dtl_01
select
    a.clue_id,
    --表中本身包含的channel_id字段值可能不准，需要从referer字段中解析出来
    case
        when a.from_source=1 then reflect("java.net.URLDecoder", "decode", regexp_extract(a.referer,'(.*channel_id=)([%0-9]+)(&.*)?',2), 'UTF-8')
        else null
    end as channel_id_1,
    --表中本身包含的channel_put字段值可能不准，需要从referer字段中解析出来
    case
        when a.from_source=1 then reflect("java.net.URLDecoder", "decode", regexp_extract(a.referer,'(.*channel_put=)([%0-9a-zA-Z\\-\\|]+)(&.*)?',2), 'UTF-8')
        else null
    end as channel_put_1,
    case
        when a.from_source=1 then reflect("java.net.URLDecoder", "decode", regexp_extract(a.referer,'(.*adid=)([%0-9a-zA-Z\\-\\|]+)(&.*)?',2), 'UTF-8')
        else null
    end as ad_id,
    case
        when a.from_source=1 then reflect("java.net.URLDecoder", "decode", regexp_extract(a.referer,'(.*creativeid=)([%0-9a-zA-Z\\-\\|]+)(&.*)?',2), 'UTF-8')
        else null
    end as creative_id,
    cast(a.channel_id as string) as channel_id,
    a.channel_put,
    a.from_source,
    a.create_time as leavephone_time
from (
    select
        clue_id,
        from_source,    -- 1 website,2 appsite
        referer,
        channel_put,
        channel_id,
        create_time
    from julive_fact.fact_site_behavior_dtl
    where 1=1
    AND channel_put is not null
    AND channel_put != ''
    AND channel_put !='-1'
    AND channel_put != ' '
    and (device_id = 2 or device_id is null) -- 来源:1pc端，2移动端
) a
left join (
    select
        clue_id,
        create_date
    from julive_dim.dim_clue_ext_base
) b on a.clue_id = b.clue_id
where 1=1
AND to_date(a.create_time)<=b.create_date
;


drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_01;

create table if not exists tmp_etl.tmp_app_feed_channelput_conversion_01 (
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
  `cost` decimal(38,4) COMMENT '消耗量'
) COMMENT 'feed渠道包转化临时表'
stored as parquet;


-- 按事件时间等纬度，统计线索量，上户量，带看量，认购量
-- 渠道要从线下渠道取
-- 线索对应城市和带看对应城市，可能不同，要分别取
insert overwrite table tmp_etl.tmp_app_feed_channelput_conversion_01
SELECT
    f.report_date, -- 报告日期
    x.city_name as customer_intent_city_name, -- 客户意向城市名称
    d.clue_id, -- 线索id
    d.media_type, -- 媒体类型
    d.channel_name, -- 渠道名称
    l.channel_put, -- 投放渠道
    sum(f.clue_num) as xs_cnt, -- 线索量
    sum(f.distribute_num) as sh_cnt, -- 上户量
    sum(f.see_num) as dk_cnt, -- 带看量
    sum(f.subscribe_num) as rg_cnt, -- 认购量:含退、含外联
    sum(f.subscribe_contains_cancel_ext_income) as rengou_yingshou, -- 认购-含退、含外联收入
    null as show_num,-- 展示
    null as click_num,-- 点击
    null as bill_cost,-- 帐面消耗
    null as cost--消耗
FROM (
    SELECT
        clue_id,
        media_name as media_type,
        channel_name
    FROM julive_dim.dim_clue_ext_base
    WHERE 1=1
    and module_name = 'feed'
    and from_source <> 2
) d
LEFT JOIN (
    SELECT
        report_date,
        clue_id,
        city_id,
        clue_num,
        distribute_num,
        see_num,
        subscribe_num,
        subscribe_contains_cancel_ext_income
    from julive_topic.topic_dws_order_city_base
    where (clue_num>0 or distribute_num>0 or see_num>0 or subscribe_num>0 or subscribe_contains_cancel_ext_income>0)
) f ON d.clue_id = f.clue_id
left join (
    select
        clue_id,
        channel_put
    from (
        select
            clue_id,
            channel_put,
            row_number() OVER(PARTITION BY clue_id ORDER BY if((channel_put='' or channel_put is null),0,1) desc,leavephone_time DESC) as r
        from (
            select
                clue_id,
                case
                    when from_source=1 then channel_put_1
                    else channel_put
                end as channel_put,
                leavephone_time
            from tmp_etl.tmp_fact_site_behavior_dtl_01
        ) a
    ) b
    where r=1
) l ON d.clue_id = l.clue_id
LEFT JOIN julive_dim.dim_city x on f.city_id = x.city_id -- 城市维度表
group by
    f.report_date,
    x.city_name,
    d.clue_id,
    d.media_type,
    d.channel_name,
    l.channel_put
;


drop table if exists tmp_etl.tmp_fact_dsp_creative_report_dtl_01;

create table if not exists tmp_etl.tmp_fact_dsp_creative_report_dtl_01 (
  `report_date` string COMMENT '报告日期',
  `dsp_account_id` int comment '市场投放账户id',
  `plan_id` bigint comment '推广计划id',
  `creative_id` bigint comment '创意id',
  `unit_id` bigint comment '推广单元id',
  `media_type` string COMMENT '媒体类型',
  `show_num` bigint COMMENT '展示',
  `click_num` bigint COMMENT '点击',
  `bill_cost` decimal(38,4) COMMENT '账面消耗',
  `channel_id` int COMMENT '渠道ID',
  `channel_put` string COMMENT '投放渠道',
  `channel_id_1` string COMMENT 'url解析渠道ID',
  `channel_put_1` string COMMENT 'url解析投放渠道'
) COMMENT '创意临时表'
stored as parquet;


-- 从创意表url中解析channel_id和channel_put
-- feed创意展，点，消从2021年开始取
insert overwrite table tmp_etl.tmp_fact_dsp_creative_report_dtl_01
SELECT
    x.report_date,
    x.dsp_account_id,
    x.plan_id,
    x.creative_id,
    x.unit_id,
    y.media_type_name,
    x.show_num,
    x.click_num,
    x.bill_cost,
    x.channel_id,
    x.channel_put,
    --表中本身包含的channel_id字段值可能不准，需要从url字段中解析出来
    reflect("java.net.URLDecoder", "decode", regexp_extract(x.url,'(.*channel_id=)([%0-9]+)(&.*)?',2), 'UTF-8') as channel_id_1,
    --表中本身包含的channel_put字段值可能不准，需要从url字段中解析出来
    reflect("java.net.URLDecoder", "decode", regexp_extract(x.url,'(.*channel_put=)([%0-9a-zA-Z\\-\\|]+)(&.*)?',2), 'UTF-8') as channel_put_1
from julive_fact.fact_dsp_creative_report_dtl x
left join julive_dim.dim_dsp_account y on x.dsp_account_id = y.id
where 1=1
and y.product_type_name = 'feed'
and x.report_date >= '2021-01-01'
;


drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_02;

create table if not exists tmp_etl.tmp_app_feed_channelput_conversion_02 (
  `report_date` string COMMENT '报告日期',
  `media_type` string COMMENT '媒体类型',
  `channel_id` string COMMENT '渠道ID',
  `channel_put` string COMMENT '投放渠道',
  `show_num` bigint COMMENT '展示',
  `click_num` bigint COMMENT '点击',
  `bill_cost` decimal(38,4) COMMENT '账面消耗',
  `cost` decimal(38,4) COMMENT '消耗量',
  `flag` int COMMENT '标识'
) COMMENT 'feed渠道包转化临时表'
stored as parquet;


-- 内连接取留电表url解析出来的channel_id和channel_put(url解析出来的更准确)，因为创意表中原有channel_id有等于999999999的
insert overwrite table tmp_etl.tmp_app_feed_channelput_conversion_02
select
    g.report_date, -- 报告日期
    g.media_type, -- 媒体类型
    l.channel_id, -- 投放渠道
    l.channel_put, -- 投放渠道
    g.show_num, -- 展示
    g.click_num, -- 点击
    g.bill_cost, -- 帐面消耗
    g.cost, --消耗
    0 as flag
from (
    SELECT
        m.report_date,
        if(m.media_type = '今日头条', cast(m.unit_id as string), null) as ad_id,
        cast(m.creative_id as string) as creative_id,
        m.media_type,
        m.show_num,
        m.click_num,
        m.bill_cost,
        m.channel_id_1 as channel_id,
        m.channel_put_1 as channel_put,
        round(m.bill_cost/(1+coalesce(n.rebate,0)),4) as cost
    FROM (
        select
            *
        from tmp_etl.tmp_fact_dsp_creative_report_dtl_01
    ) m
    left join (
        select
            dsp_account_id,
            rebate_date,
            rebate
        from julive_dim.dim_dsp_account_rebate
    ) n
    on m.dsp_account_id=n.dsp_account_id and m.report_date =  n.rebate_date
) g
inner join (
    select
        distinct ad_id,
        creative_id,
        channel_id_1 as channel_id,
        channel_put_1 as channel_put
    from tmp_etl.tmp_fact_site_behavior_dtl_01
    where 1=1
    and length(ad_id)>0
    and length(creative_id)>0
) l on l.ad_id=g.ad_id and l.creative_id=g.creative_id
where 1=1
and l.channel_put is not null
;


-- 外连接取创意表url解析出来的channel_id和channel_put(url解析出来的更准确)，因为创意表中原有channel_id有等于999999999的
insert into table tmp_etl.tmp_app_feed_channelput_conversion_02
select
    g.report_date, -- 报告日期
    g.media_type, -- 媒体类型
    g.channel_id, -- 投放渠道
    g.channel_put, -- 投放渠道
    g.show_num, -- 展示
    g.click_num, -- 点击
    g.bill_cost, -- 帐面消耗
    g.cost, --消耗
    1 as flag
from (
    SELECT
        m.report_date,
        if(m.media_type = '今日头条', cast(m.unit_id as string), null) as ad_id,
        cast(m.creative_id as string) as creative_id,
        m.media_type,
        m.show_num,
        m.click_num,
        m.bill_cost,
        m.channel_id_1 as channel_id,
        m.channel_put_1 as channel_put,
        round(m.bill_cost/(1+coalesce(n.rebate,0)),4) as cost
    FROM (
        select
            *
        from tmp_etl.tmp_fact_dsp_creative_report_dtl_01
    ) m
    left join (
        select
            dsp_account_id,
            rebate_date,
            rebate
        from julive_dim.dim_dsp_account_rebate
    ) n
    on m.dsp_account_id=n.dsp_account_id and m.report_date =  n.rebate_date
) g
left join (
    select
        distinct ad_id,
        creative_id,
        channel_id_1 as channel_id,
        channel_put_1 as channel_put
    from tmp_etl.tmp_fact_site_behavior_dtl_01
    where 1=1
    and length(ad_id)>0
    and length(creative_id)>0
) l on l.ad_id=g.ad_id and l.creative_id=g.creative_id
where 1=1
and l.channel_put is null
and length(g.channel_put)>0
;



drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_03;

create table if not exists tmp_etl.tmp_app_feed_channelput_conversion_03 (
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
  `cost` decimal(38,4) COMMENT '消耗量'
) COMMENT 'feed渠道包转化临时表'
stored as parquet;


-- 按创意日期等维度统计展，点，消
-- 渠道要从线上渠道取
insert overwrite table tmp_etl.tmp_app_feed_channelput_conversion_03
select
    g.report_date, -- 报告日期
    t3.city_name as customer_intent_city_name, -- 城市名称
    0 as clue_id, -- 线索id
    g.media_type, -- 媒体类型
    t3.channel_name, -- 渠道名称
    g.channel_put, -- 投放渠道
    null as xs_cnt, -- 线索量
    null as sh_cnt, -- 上户量
    null as dk_cnt, -- 带看量
    null as rg_cnt, -- 认购量:含退、含外联
    null as rengou_yingshou, -- 认购-含退、含外联收入
    sum(g.show_num) as show_num, -- 展示
    sum(g.click_num) as click_num, -- 点击
    sum(g.bill_cost) as bill_cost, -- 帐面消耗
    sum(g.cost) as cost  --消耗
from tmp_etl.tmp_app_feed_channelput_conversion_02 g
left join julive_dim.dim_channel_info t3 on g.channel_id = cast(t3.channel_id as string)
group by
    g.report_date,
    t3.city_name,
    g.media_type,
    t3.channel_name,
    g.channel_put
;


INSERT OVERWRITE TABLE julive_app.app_feed_channelput_conversion
SELECT
    d.report_date, -- 报告日期
    d.customer_intent_city_name, -- 城市名称
    d.clue_id, -- 线索id
    d.media_type, -- 媒体类型
    d.channel_name, -- 渠道名称
    d.channel_put, -- 投放渠道
    sum(d.clue_num), -- 线索量
    sum(d.distribute_num), -- 上户量
    sum(d.see_num), -- 带看量
    sum(d.subscribe_num), -- 认购量:含退、含外联
    sum(d.subscribe_contains_cancel_ext_income), -- 认购-含退、含外联收入
    sum(d.show_num), -- 展示
    sum(d.click_num), -- 点击
    sum(d.bill_cost), -- 帐面消耗
    sum(d.cost), --消耗
    current_timestamp as etl_time -- ETL跑数时间
FROM (
    select
        *
    from tmp_etl.tmp_app_feed_channelput_conversion_01
    union all
    select
        *
    from tmp_etl.tmp_app_feed_channelput_conversion_03
) d
group by
    d.report_date,
    d.customer_intent_city_name,
    d.clue_id,
    d.media_type,
    d.channel_name,
    d.channel_put
;

-- 删除临时表
-- drop table if exists tmp_etl.tmp_fact_site_behavior_dtl_01;
-- drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_01;
-- drop table if exists tmp_etl.tmp_fact_dsp_creative_report_dtl_01;
-- drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_02;
-- drop table if exists tmp_etl.tmp_app_feed_channelput_conversion_03;







