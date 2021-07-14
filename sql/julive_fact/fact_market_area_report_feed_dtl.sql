set hive.execution.engine=spark;
set spark.app.name=fact_market_area_report_dtl;
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
--   信息流数据
--   			ods.dsp_feed_area_stat 信息流区域报告 9点/10点/14点
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

--   对于应用商店(应用市场（苹果）、应用市场（安卓）)媒体数据，投放不区分地域，需要拆分到地市处理
--   拆分算法： 见任务 fact_market_appstore_city_share

--   8 点 依赖
--         依赖任务 
--         fact_dsp_sem_keyword_report_day_dtl_0800  802
--           

--   10 点 依赖
--         8点任务
--         dsp_jrtt_ad_plan_report  766
--         fact_dsp_sem_keyword_report_day_dtl_0800  802
--         fact_android_app_store_cost_indi   2903
--         dsp_feed_area_stat_1030  2368
--         dsp_sem_area_report      597
--         dsp_creative_report_0900 1710


--    12 点 
--          10点任务
--          dsp_app_data_import_data  945 12点
--          dsp_feed_area_stat_1130   697  11:30
--          fact_dsp_sem_keyword_report_day_dtl_1100  823
--          fact_market_appstore_city_share 2927

--    14 点 依赖 12点任务


-- 第三部分 - FEED数据
-- ods.dsp_feed_area_stat 

INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
    -- feed - APP渠道 小程序
    SELECT 
        from_unixtime(feed_area.report_date,'yyyy-MM-dd') as report_date,
        from_unixtime(feed_area.report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
        feed_area.data_from,
        if(deal_dsp_account.media_type_name like '应用市场%' ,'应用市场','feed') as media_class,
        if(deal_dsp_account.product_type_name='APP渠道',deal_dsp_account.product_type_name,
           if(deal_dsp_account.media_type in (5,25) and deal_dsp_account.product_type_name='小程序','微信小程序','M')) as device_name,
        feed_area.account_id,
        feed_area.show_num,
        feed_area.click_num,
        feed_area.bill_cost,
        coalesce(rwd.bill_reward,0) * (feed_area.bill_cost / feed_area.total_bill_cost) as reward,
        (feed_area.bill_cost - coalesce(rwd.bill_reward,0) * (feed_area.bill_cost / feed_area.total_bill_cost)) / (1 +  coalesce(dsp_account_rebate.rebate,0)) as cost,
        feed_area.download_num,
        feed_area.plan_id,
        feed_area.plan_name,
        regexp_extract(feed_area.city_name, '(.*?)(市|$)', 1) as city_name,
        null as channel_id ,
        null as channel_name  ,
        null as channel_city_name ,
        null as url_city_name ,
        null as kw_city_name ,
        if(deal_dsp_account.product_type_name='APP渠道', deal_dsp_account.app_type,'M') as app_type ,
        feed_area.creator,
        feed_area.updator,
        feed_area.create_datetime,
        feed_area.update_datetime,
        current_timestamp() as etl_time,
        from_unixtime(feed_area.report_date,'yyyyMMdd') as pdate,
        'feed' as source
    FROM (
        select
            cur_date as report_date,
            'dsp_feed_area_stat' as data_from,
            dsp_account_id as account_id,
            `show` as show_num,
            click as click_num,
            cost as bill_cost,     
            sum(cost) over (partition by cur_date,dsp_account_id) as total_bill_cost,
            download_finish as download_num,
            0 as plan_id,
            '0' as plan_name,
            -- 城市数据处理
            if(regexp_extract(city_name, '(.*?)(市|$)', 1) in ('北京','上海','广州','天津','苏州','杭州','成都','重庆','深圳','郑州','南京',
               '佛山','全国','西安','长沙','武汉','宁波','沈阳','济南','青岛','大连','烟台',
               '长春','合肥','石家庄','厦门','嘉兴','无锡','湖州','镇江','惠州','东莞','中山',
               '绵阳','廊坊','湘潭','岳阳','胶州','常熟','贵阳','昆明','眉山','简阳','咸阳',
               '清远','珠海'),regexp_extract(city_name, '(.*?)(市|$)', 1),null) as city_name,
            creator,
    	      updator,
            create_datetime,
            update_datetime
      from ods.dsp_feed_area_stat 
      where data_type != 2 -- data_type 数据类型 1.按区域统计，2.按账户统计
            and (media_type <> 23 or cur_date >= unix_timestamp('2021-05-20','yyyy-MM-dd')) -- 快手取20210520 之后api的数据 
            and (media_type not in (5, 25) or cur_date >= unix_timestamp('2021-06-24','yyyy-MM-dd')) -- 微信公众号/广点通-APP渠道 取20210624 之后api的数据
            and cur_date = unix_timestamp(${hiveconf:etl_date},'yyyyMMdd')
    ) feed_area
    LEFT JOIN julive_dim.dim_dsp_account_rebate dsp_account_rebate on feed_area.account_id=dsp_account_rebate.dsp_account_id
              and feed_area.report_date = dsp_account_rebate.rebate_timestamp
    JOIN julive_dim.dim_dsp_account_history deal_dsp_account 
              on feed_area.account_id = deal_dsp_account.id  
              and from_unixtime(feed_area.report_date,'yyyyMMdd') = deal_dsp_account.pdate
    LEFT JOIN (
        select a.cur_date,
               a.dsp_account_id,
               sum(a.reward_cost) as bill_reward
        from ods.dsp_account_daily_cost a 
        where a.reward_cost <> 0  
              and from_unixtime(a.cur_date,'yyyyMMdd')=${hiveconf:etl_date}
        group by a.cur_date , a.dsp_account_id
    ) rwd on feed_area.report_date = rwd.cur_date and feed_area.account_id = rwd.dsp_account_id
    where deal_dsp_account.product_type_name in ('APP渠道','小程序')

UNION ALL
    -- feed - feed
    select
        from_unixtime(report_date,'yyyy-MM-dd') as report_date,
        from_unixtime(report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
        'dsp_creative_report' as data_from,
        'feed' as media_class,
        device_name,
        account_id,
        show_num,
        click_num,
        bill_cost,
        coalesce(bill_reward,0) * (bill_cost / total_bill_cost) as reward,
        cost - coalesce(actual_reward,0) * (bill_cost / total_bill_cost) as cost,
        0 as download_num,
        plan_id,
        plan_name,
        regexp_extract(city_name, '(.*?)(市|$)', 1) city_name,
        null as channel_id ,
        null as channel_name  ,
        null as channel_city_name ,
        null as url_city_name ,
        null as kw_city_name ,
        app_type ,
        creator,
        updator,
        create_datetime,
        update_datetime,
        current_timestamp() as etl_time,
        from_unixtime(report_date,'yyyyMMdd') as pdate,
        'feed' as source
     from (
        select
            dcr.report_date,
            dcr.pdate,
            dcr.account_id,
            dcr.show_num,
            dcr.click_num,
            dcr.bill_cost,
            sum(dcr.bill_cost) over (partition by dcr.report_date,dcr.account_id) as total_bill_cost,
            rwd.bill_reward,
            rwd.actual_reward,
            dcr.cost,
            dcr.plan_id,
            dcr.plan_name,
            dcr.creator,
            dcr.updator,
            dcr.create_datetime,
            dcr.update_datetime,
            dsp_account.city as city_name,
            if(dsp_account.product_type_name='APP渠道', dsp_account.product_type_name,'M') as device_name,
            if(dsp_account.product_type_name='APP渠道', dsp_account.app_type,'M')  as app_type
        from (
            SELECT
                report_date,
                from_unixtime(report_date,'yyyyMMdd') as pdate,
                dsp_account_id as account_id,
                sum(show_num) as show_num,
                sum(click_num) as click_num,
                sum(bill_cost) as bill_cost,
                sum(cost) as cost,
                plan_id,
                plan_name,
                creator,
                updator,
                max(create_datetime) as create_datetime,
                max(update_datetime) as update_datetime
            from ods.dsp_creative_report 
            where report_date = unix_timestamp(${hiveconf:etl_date},'yyyyMMdd')
            group by report_date,
                from_unixtime(report_date,'yyyyMMdd'),
                dsp_account_id,
                plan_id,
                plan_name,
                creator,
                updator
        )dcr
        JOIN julive_dim.dim_dsp_account_history dsp_account 
              on dcr.account_id = dsp_account.id  
              and from_unixtime(dcr.report_date,'yyyyMMdd') = dsp_account.pdate
        LEFT JOIN ( 
        
            select a.cur_date,
                   a.dsp_account_id,
                   sum(a.reward_cost) as bill_reward,
                   sum(a.reward_cost /(1+ coalesce(rebate.rebate,0))) as actual_reward
            from ods.dsp_account_daily_cost a 
            left join julive_dim.dim_dsp_account_rebate rebate on a.dsp_account_id=rebate.dsp_account_id 
                 and a.cur_date =  rebate.rebate_timestamp
            where a.reward_cost <> 0  
                  and a.cur_date = unix_timestamp(${hiveconf:etl_date},'yyyyMMdd')
            group by a.cur_date, a.dsp_account_id
             
             
        ) rwd on dcr.report_date = rwd.cur_date and dcr.account_id = rwd.dsp_account_id
        where dsp_account.product_type_name='feed'
              and dsp_account.media_type_name != '今日头条'
    ) tab
    
    -- 今日头条数据
    UNION ALL
     
        
select 
    from_unixtime(jrtt.report_date,'yyyy-MM-dd') as report_date,
    from_unixtime(jrtt.report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
    jrtt.data_from,
    if(acc.media_type_name like '应用市场%' ,'应用市场','feed') as media_class,
    if(acc.product_type_name='APP渠道', acc.product_type_name,'M') as device_name,
    jrtt.account_id,
    jrtt.show_num,
    jrtt.click_num,
    jrtt.bill_cost,
    coalesce(rwd.bill_reward,0) * (jrtt.bill_cost / jrtt.total_bill_cost) as reward,
    (jrtt.bill_cost - coalesce(rwd.bill_reward,0) * (jrtt.bill_cost / jrtt.total_bill_cost)) / (1 +  coalesce(rebate.rebate,0)) as cost,
    0 as download_num,
    jrtt.campaign_id as plan_id,
    jrtt.campaign_name as plan_name,
    regexp_extract(jrtt.city_name, '(.*?)(市|$)', 1) city_name,
    null as channel_id ,
    null as channel_name  ,
    null as channel_city_name ,
    null as url_city_name ,
    null as kw_city_name ,
    if(acc.product_type_name='APP渠道', acc.app_type,'M') as app_type ,
    jrtt.creator,
    jrtt.updator,
    jrtt.create_datetime,
    jrtt.update_datetime,
    current_timestamp() as etl_time,
    from_unixtime(jrtt.report_date,'yyyyMMdd') as pdate,
    'feed' as source
from(
    select
        a.account_id,                                                                      
        a.city_name,                                                              
        a.report_date,
        a.campaign_id,
        a.campaign_name,                                                                                                                         
        sum(a.show_num) as show_num,                                                                                                                                     
        sum(a.click_num) as click_num,                                                                                                                                                                                                                                                                
        sum(a.bill_cost) as bill_cost,
        total_bill_cost,
        max(a.creator) as creator,
    	  max(a.updator) as updator,
        max(a.create_datetime) as create_datetime,
        max(a.update_datetime) as  update_datetime,
        'dsp_jrtt_ad_plan_report'  as data_from             
    from(                                                                                                                                                 
        select                                                                                                                                           
            case                                                                                                                                         
            when campaign_name like '%天津%' then'天津'                                                                                                      
            when campaign_name like '%北京%' then'北京'                                                                                                      
            when campaign_name like '%上海%' then'上海'                                                                                                      
            when campaign_name like '%苏州%' then'苏州'                                                                                                      
            when campaign_name like '%杭州%' then'杭州'                                                                                                      
            when campaign_name like '%南京%' then'南京'                                                                                                      
            when campaign_name like '%广州%' then'广州'                                                                                                      
            when campaign_name like '%深圳%' then'深圳'                                                                                                      
            when campaign_name like '%重庆%' then'重庆'                                                                                                      
            when campaign_name like '%成都%' then'成都'                                                                                                      
            when campaign_name like '%郑州%' then'郑州'                                                                                                      
            when campaign_name like '%佛山%' then'佛山'                                                                             
            when campaign_name like '%长沙%' then'长沙'
            when campaign_name like '%武汉%' then'武汉'
            when campaign_name like '%西安%' then'西安'
            when campaign_name like '%宁波%' then'宁波'
            when campaign_name like '%济南%' then'济南'
            when campaign_name like '%沈阳%' then'沈阳'
            when campaign_name like '%青岛%' then'青岛'
            when campaign_name like '%大连%' then'大连'
            when campaign_name like '%烟台%' then'烟台'
            when campaign_name like '%长春%' then'长春'
            when campaign_name like '%合肥%' then'合肥'
            when campaign_name like '%石家庄%' then'石家庄'
            when campaign_name like '%厦门%' then'厦门'
            when campaign_name like '%嘉兴%' then'嘉兴'
            when campaign_name like '%无锡%' then'无锡'
            when campaign_name like '%湖州%' then'湖州'
            when campaign_name like '%镇江%' then'镇江'
            when campaign_name like '%惠州%' then'惠州'
            when campaign_name like '%东莞%' then'东莞'
            when campaign_name like '%中山%' then'中山'
            when campaign_name like '%绵阳%' then'绵阳'
            when campaign_name like '%廊坊%' then'廊坊'
            when campaign_name like '%湘潭%' then'湘潭'
            when campaign_name like '%岳阳%' then'岳阳'
            when campaign_name like '%胶州%' then'胶州'
            when campaign_name like '%常熟%' then'常熟'
            when campaign_name like '%贵阳%' then'贵阳'
            when campaign_name like '%昆明%' then'昆明'
            when campaign_name like '%眉山%' then'眉山'
            when campaign_name like '%简阳%' then'简阳'
            when campaign_name like '%咸阳%' then'咸阳'
            when campaign_name like '%清远%' then'清远'
            when campaign_name like '%珠海%' then'珠海'
            else null end as city_name,
            campaign_id,
            campaign_name,
            account_id,
            report_date ,
            creator,
    	      updator,
            create_datetime,
            update_datetime,
            sum(cost) over (partition by account_id,report_date) as total_bill_cost,
            `show` as show_num,
            click as click_num,
            cost  as bill_cost
        from ods.dsp_jrtt_ad_plan_report -- 今日头条 plan
        where pdate = ${hiveconf:etl_date}
    ) a
    group by  
        account_id,
        city_name,
        report_date,
        campaign_id,
        campaign_name,
        total_bill_cost
) jrtt   
JOIN julive_dim.dim_dsp_account_history acc   on jrtt.account_id = acc.id   and from_unixtime(jrtt.report_date,'yyyyMMdd') = acc.pdate
left join julive_dim.dim_dsp_account_rebate rebate on jrtt.account_id=rebate.dsp_account_id and jrtt.report_date =  rebate.rebate_timestamp
LEFT JOIN (
        select a.cur_date,
               a.dsp_account_id,
               sum(a.reward_cost) as bill_reward
        from ods.dsp_account_daily_cost a 
        where a.reward_cost <> 0  
              and from_unixtime(a.cur_date,'yyyyMMdd')=${hiveconf:etl_date}
        group by a.cur_date , a.dsp_account_id
    ) rwd on jrtt.report_date = rwd.cur_date and jrtt.account_id = rwd.dsp_account_id
where acc.media_type_name='今日头条'
      and acc.product_type_name='feed'
;
    

-- 修复 返点数据

-- julive_fact.fact_market_area_report_dtl

INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
select 
    a.report_date,
    a.report_time,
    a.data_from,
    a.media_class,
    a.device_name,
    a.account_id,
    a.show_num,
    a.click_num,
    a.bill_cost,
    a.reward,
    (a.bill_cost - coalesce(a.reward))/(1 +  coalesce(rebate.rebate,0)) as  cost,
    a.download_num,
    a.plan_id,
    a.plan_name,
    a.city_name,
    a.channel_id,
    a.channel_name,
    a.channel_city_name,
    a.url_city_name,
    a.kw_city_name,
    a.app_type,
    a.creator,
    a.updator,
    a.create_datetime,
    a.update_datetime,
    a.etl_time,
    a.pdate,
    a.source
from julive_fact.fact_market_area_report_dtl a 
left join julive_dim.dim_dsp_account_rebate rebate on a.account_id=rebate.dsp_account_id and a.report_date =  rebate.rebate_date
where source = 'feed'
and pdate >= date_format(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-100),'yyyyMMdd')
;