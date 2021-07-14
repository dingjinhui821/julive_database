set hive.execution.engine=spark;
set spark.app.name=fact_market_area_sem_report_dtl;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=3g;
set spark.executor.instances=12;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

set etl_date = '${hiveconf:etlDate}';
-- set etl_yestoday = '${hiveconf:etlYestoday}'; 
-- set etl_date = '20210101';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：市场区域报告 -- 展点销
--   输 入 表 ：
--   SEM 数据
---        julive_fact.fact_dsp_sem_keyword_report_day_dtl --sem关键字报告 1点/11点更新 
--         ods.dsp_sem_area_report  --sem地域报告      
--   其它输入表：
--        ods.hj_channel_plan_relation                -- 
--        julive_dim.dim_dsp_account_rebate           -- 返点数据
--        julive_dim.dim_dsp_account_history          -- 账户表
--        julive_dim.dim_channel_info                 -- 渠道码表
--        julive_dim.dim_city                         -- 城市维度表
--   输 出 表：julive_fact.fact_market_area_report_dtl
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/05/19 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

--   8 点 依赖
--         依赖任务 
--         fact_dsp_sem_keyword_report_day_dtl_0800  802

--   10 点 依赖
--         8点任务
--         dsp_jrtt_ad_plan_report  766
--         fact_dsp_sem_keyword_report_day_dtl_0800  802
--         dsp_feed_area_stat_1030  2368
--         dsp_creative_report_0900 1710

--    12 点 
--          10点任务
--          dsp_sem_area_report      597
--          fact_android_app_store_cost_indi   2903
--          dsp_app_data_import_data  945 12点
--          dsp_feed_area_stat_1130   697  11:30
--          fact_dsp_sem_keyword_report_day_dtl_1100  823
--          fact_market_appstore_city_share 2927

--    14 点 依赖 12点任务

    
-- 第四部分 - SEM数据 -竞品词等
INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
SELECT 
      from_unixtime(a.report_date,'yyyy-MM-dd') as report_date,
      from_unixtime(a.report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
      'dsp_sem_area_report' as data_from,
      'SEM' as media_class,
      --if(dsp_account.product_type_name='APP渠道', dsp_account.product_type_name,
      --     IF(dsp_sem_area_report.device = 1,'PC',IF(dsp_sem_area_report.device = 2,'M',dsp_sem_area_report.device)) ) as device_name,
      'APP渠道' as device_name,
      a.dsp_account_id as account_id,
      a.show_num as show_num,
      a.click_num as click_num,
      a.bill_cost as bill_cost,
      null as reward,
      round(a.bill_cost/(1+coalesce(rebate.rebate,0)),4) as cost,
      0 as download_num,
      a.plan_id,
      a.plan_name,
      -- coalesce(dim_city.city_name,'其他') as city_name,
      case when a.city in (
      '北京','上海','广州','天津','苏州','杭州','成都','重庆','深圳','郑州','南京',
      '佛山','全国','西安','长沙','武汉','宁波','沈阳','济南','青岛','大连','烟台','长春',
      '合肥','石家庄','厦门','嘉兴','无锡','湖州','镇江','惠州','东莞','中山','绵阳','廊坊',
      '湘潭','岳阳','胶州','常熟','贵阳','昆明','眉山','简阳','咸阳','清远','珠海') 
      then a.city else '其他' end as city_name,
      null as channel_id ,
      null as channel_name  ,
      null as channel_city_name ,
      null as url_city_name ,
      null as kw_city_name ,
      --if(dsp_account.product_type_name='APP渠道', dsp_account.app_type,
      --     IF(dsp_sem_area_report.device = 1,'PC',IF(dsp_sem_area_report.device = 2,'M',dsp_sem_area_report.device)) ) as app_type ,
      dsp_account.app_type,
      a.creator,
      a.updator,
      a.create_datetime,
      a.update_datetime,
      current_timestamp() as etl_time ,
      from_unixtime(report_date,'yyyyMMdd') as pdate,
     'SEM_APP2' as source
   FROM ods.dsp_sem_area_report a
   -- ？此处为什么不能直接关联dim_city表？
   -- LEFT JOIN julive_dim.dim_city ON dsp_sem_area_report.city = dim_city.city_name
   left join julive_dim.dim_dsp_account_rebate rebate on a.dsp_account_id=rebate.dsp_account_id and a.report_date =  rebate.rebate_timestamp
   JOIN julive_dim.dim_dsp_account_history dsp_account on a.dsp_account_id = dsp_account.id  and dsp_account.p_date =  from_unixtime(a.report_date,'yyyy-MM-dd')
   WHERE a.dsp_account_id in (538,270,143,45,42,535,7,226)
      and a.plan_name in ('APP下载', '品牌词', '竞品词', 'APP-应用推广')
      and a.pdate = ${hiveconf:etl_date}
;

-- 第五部分 - SEM数据
INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
      SELECT 
         report_date as report_date,
         from_unixtime(unix_timestamp(report_date,'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss') as report_time,
         'fact_dsp_sem_keyword_report_day_dtl' as data_from,
         'SEM' as media_class,
         -- if(acc.product_type_name='APP渠道', acc.product_type_name, sac.device_name) as device_name,
         'APP渠道' as device_name,
         sac.account_id,
         sum(sac.show_num) as show_num,
         sum(sac.click_num) as click_num,
         sum(sac.bill_cost) as bill_cost,
         null as reward,
         sum(sac.cost) as cost,
         0 as download_num,
         sac.plan_id,
         sac.plan_name,
         coalesce(sac.url_city_name,sac.channel_city_name) as city_name,
         sac.channel_id ,
         sac.channel_name,
         sac.channel_city_name ,
         sac.url_city_name ,
         null as kw_city_name ,
         sac.app_type,
         0 as creator ,
         0 as updator ,
         0 as create_datetime ,
         0 as update_datetime ,
         current_timestamp() as etl_time,
         sac.pdate as pdate,
         'SEM' as source
     FROM (

           SELECT
                coalesce(channel_c.channel_id,channel_a.channel_id) AS channel_id,
                channel.channel_name,
                channel.app_type_name as app_type,
                channel.city_name AS channel_city_name,
                if(kwd.channel_id is null,null,url_city.city_name) AS url_city_name,                
                kwd.account_id,
                kwd.plan_id,
                kwd.plan_name,
                kwd.report_date,
                kwd.device_id AS device,
                IF(kwd.device_id = 1,'PC',IF(kwd.device_id = 2,'M',kwd.device_id)) as device_name ,
                kwd.show_num,
                kwd.click_num,
                kwd.bill_cost,
                cost,
                kwd.pdate
            FROM
            (
                SELECT
                    if(a.channel_id = 999999999,NULL,a.channel_id) AS channel_id,
                    a.account_id,
                    a.plan_id,
                    a.plan_name,
                    a.report_date,
                    a.device_id,
                    a.show_num,
                    a.click_num,
                    a.bill_cost,
                    round(a.bill_cost/(1+coalesce(rebate.rebate,0)),4) as cost,
                    a.pdate,
                    a.url
                FROM julive_fact.fact_dsp_sem_keyword_report_day_dtl a 
                left join julive_dim.dim_dsp_account_rebate rebate on a.account_id=rebate.dsp_account_id and a.report_date =  rebate.rebate_date
                WHERE a.pdate = ${hiveconf:etl_date}
                      and a.account_id in (538,270,143,45,42,535,7,226)
                      and a.plan_name not in ('APP下载', '品牌词', '竞品词', 'APP-应用推广')
            ) kwd
            LEFT JOIN 
            (
                SELECT
                    plan_id,
                    channel_id
                FROM ods.hj_channel_plan_relation
                WHERE  plan_id != 0
            )channel_c ON kwd.plan_id = channel_c.plan_id
            LEFT JOIN 
            (
                SELECT
                    account_id,
                    channel_id
                FROM ods.hj_channel_plan_relation
                WHERE plan_id = 0
            )channel_a ON kwd.account_id = channel_a.account_id
            LEFT JOIN julive_dim.dim_channel_info channel 
            ON coalesce(channel_c.channel_id,channel_a.channel_id) = channel.channel_id
            LEFT JOIN tmp_bi.url_city ON trim(kwd.url) = url_city.url
    ) sac
    JOIN julive_dim.dim_dsp_account_history acc on sac.account_id = acc.id  and acc.p_date =  sac.report_date
    group by report_date,
         sac.pdate,
         sac.account_id,
         sac.plan_id,
         sac.plan_name,
         coalesce(sac.url_city_name,sac.channel_city_name),
         sac.channel_id ,
         sac.channel_name,
         sac.channel_city_name ,
         sac.url_city_name ,
         sac.app_type

   UNION ALL
   SELECT 
         report_date as report_date,
         from_unixtime(unix_timestamp(report_date,'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss') as report_time,
         'fact_dsp_sem_keyword_report_day_dtl' as data_from,
         'SEM' as media_class,
         if(acc.product_type_name in ('小程序','APP渠道'), acc.product_type_name, sac.device_name) as device_name,
         sac.account_id,
         sum(sac.show_num) as show_num,
         sum(sac.click_num) as click_num,
         sum(sac.bill_cost) as bill_cost,
         null as reward,
         sum(sac.cost) as cost,
         0 as download_num,
         sac.plan_id,
         sac.plan_name,
         coalesce(sac.url_city_name,sac.channel_city_name) as city_name,
         sac.channel_id ,
         sac.channel_name,
         sac.channel_city_name ,
         sac.url_city_name ,
         null as kw_city_name ,
         device_name as app_type,
         0 as creator ,
         0 as updator ,
         0 as create_datetime ,
         0 as update_datetime ,
         current_timestamp() as etl_time,
         sac.pdate as pdate,
         'SEM' as source
     FROM (

           SELECT
                coalesce(kwd.channel_id,channel_c.channel_id,channel_a.channel_id) AS channel_id,
                channel.channel_name,
                channel.city_name AS channel_city_name,
                channel.app_type_name as app_type,
                if(kwd.channel_id is null,null,url_city.city_name) AS url_city_name,                
                kwd.account_id,
                kwd.plan_id,
                kwd.plan_name,
                kwd.report_date,
                kwd.device_id AS device,
                IF(kwd.device_id = 1,'PC',IF(kwd.device_id = 2,'M',kwd.device_id)) as device_name ,
                kwd.show_num,
                kwd.click_num,
                kwd.bill_cost,
                cost,
                kwd.pdate
            FROM
            (
                SELECT
                    if(a.channel_id = 999999999,NULL,a.channel_id) AS channel_id,
                    a.account_id,
                    a.plan_id,
                    a.plan_name,
                    a.report_date,
                    a.device_id,
                    a.show_num,
                    a.click_num,
                    a.bill_cost,
                    round(a.bill_cost/(1+coalesce(rebate.rebate,0)),4) as cost,
                    a.pdate,
                    a.url
                FROM julive_fact.fact_dsp_sem_keyword_report_day_dtl a
                left join julive_dim.dim_dsp_account_rebate rebate on a.account_id=rebate.dsp_account_id and a.report_date =  rebate.rebate_date
                WHERE a.pdate = ${hiveconf:etl_date}
                      and a.account_id not in (538,270,143,45,42,535,7,226)
            ) kwd
            LEFT JOIN 
            (
                SELECT
                    plan_id,
                    channel_id
                FROM ods.hj_channel_plan_relation
                WHERE  plan_id != 0
            )channel_c ON kwd.plan_id = channel_c.plan_id
            LEFT JOIN 
            (
                SELECT
                    account_id,
                    channel_id
                FROM ods.hj_channel_plan_relation
                WHERE plan_id = 0
            )channel_a ON kwd.account_id = channel_a.account_id
            LEFT JOIN julive_dim.dim_channel_info  channel 
            ON coalesce(kwd.channel_id,channel_c.channel_id,channel_a.channel_id) = channel.channel_id
            LEFT JOIN tmp_bi.url_city ON trim(kwd.url) = url_city.url
    ) sac
    JOIN julive_dim.dim_dsp_account_history acc on sac.account_id = acc.id  and acc.p_date =  sac.report_date
    group by report_date,
         sac.pdate,
         sac.account_id,
         sac.plan_id,
         sac.plan_name,
         coalesce(sac.url_city_name,sac.channel_city_name),
         sac.channel_id ,
         sac.channel_name,
         sac.channel_city_name ,
         sac.url_city_name ,
         if(acc.product_type_name in ('小程序','APP渠道'), acc.product_type_name, sac.device_name),
         sac.device_name
;