set hive.execution.engine=spark;
set spark.app.name=fact_market_area_import_report_dtl;
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

--   功能描述：市场区域报告 -手工导入- 展点销
--   输 入 表 ：
--   app应用商店：
--         ojulive_fact.fact_android_app_store_cost_indi             -- 华为 oppo vivo xiaomi 展点消明细
--   手工导入数据：
--   			ods.dsp_app_data_import_data 手工导入/12点 
--   其它输入表：
--        julive_fact.fact_market_appstore_city_share -- 应用市场城市分摊表
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

 
-- 第一部分 - 应用商店数据  -数据量10w级 ，每次更新20210101全量数据
with app_store_cost as (
SELECT
	report_date,
	cast(account_id as int) as account_id,
	media_type_name,
	show_num,
	click_num,
	bill_cost,
	download_num,
	cast(plan_id as int) as plan_id,
	plan_name,
	0 as creator ,
  0 as updator ,
  0 as create_datetime ,
  0 as update_datetime 
FROM  julive_fact.fact_android_app_store_cost_indi
WHERE report_date>='2021-01-01'  
)
    
--第一部分 应用市场 
INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
SELECT 
  from_unixtime(app_store.report_date,'yyyy-MM-dd') as report_date,
  from_unixtime(app_store.report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
  app_store.data_from,
  if(deal_dsp_account.media_type_name like '应用市场%' ,'应用市场','feed') as media_class,
  if(deal_dsp_account.product_type_name='APP渠道', deal_dsp_account.product_type_name,'M') as device_name,
  app_store.account_id,
  app_store.show_num,
  app_store.click_num,
  app_store.bill_cost,
  null as reward,
  app_store.bill_cost / (1 +  coalesce(dsp_account_rebate.rebate,0)) as cost,
  app_store.download_num,
  app_store.plan_id,
  app_store.plan_name,
  app_store.city_name,
  null as channel_id ,
  null as channel_name  ,
  null as channel_city_name ,
  null as url_city_name ,
  null as kw_city_name ,
  if(deal_dsp_account.product_type_name='APP渠道', deal_dsp_account.app_type,'M') as app_type ,
  app_store.creator,
  app_store.updator,
  app_store.create_datetime,
  app_store.update_datetime,
  current_timestamp() as etl_time,
  from_unixtime(app_store.report_date,'yyyyMMdd') as pdate,
  'appstore' as source
FROM (
    select 
        p1.record_date as report_date,
        p1.data_from ,
        p1.account_id ,
        p1.show_num ,
        p1.click_num ,
        p1.bill_cost * city_cost_p.city_rate as bill_cost,
        p1.download_num ,
        p1.plan_id ,
        p1.plan_name ,
        city_cost_p.city_name as city_name,
        p1.creator,
        p1.updator,
        p1.create_datetime ,
        p1.update_datetime 
   FROM (
       SELECT
           record_date,
           'dsp_app_data_import_data' as data_from,
           dsp_app_data_import_data.account_id ,
           deal_dsp_account.media_type_name,
           0 as show_num,
	     	   0 as click_num,
	     	   cash_cost as bill_cost,
	     	   0 as download_num,
           0 as plan_id,
	     	   '0' as plan_name,
	         dsp_app_data_import_data.creator,
	         dsp_app_data_import_data.updator,
           dsp_app_data_import_data.create_datetime,
           dsp_app_data_import_data.update_datetime
       from ods.dsp_app_data_import_data
       join julive_dim.dim_dsp_account_history deal_dsp_account on dsp_app_data_import_data.account_id = deal_dsp_account.id 
            and deal_dsp_account.pdate =  from_unixtime(record_date,'yyyyMMdd') 
       WHERE ((city_name = '全国' or city_name = '其它')
             and ((instr(deal_dsp_account.agent,'huawei')=0
             and instr(deal_dsp_account.agent,'xiaomi')=0
             and instr(deal_dsp_account.agent,'oppo')=0
             and instr(deal_dsp_account.agent,'vivo')=0)
             or instr(deal_dsp_account.account,'shoulu')>0
             or from_unixtime(record_date,'yyyy-MM-dd') <= '2019-12-31'
             ))
             and (dsp_app_data_import_data.media_type <> 23 
                  or dsp_app_data_import_data.record_date < unix_timestamp('2021-05-20','yyyy-MM-dd')) -- 快手取20210520 之前的手工数据
             and from_unixtime(dsp_app_data_import_data.record_date,'yyyy-MM-dd') >= '2021-01-01'
	    UNION ALL
	    SELECT
          unix_timestamp(report_date,'yyyy-MM-dd') as record_date,
          'fact_android_app_store_cost_indi' as data_from,
          account_id ,
          media_type_name,
          0 as show_num,
	        0 as click_num,
	        bill_cost,
	        download_num,
	        plan_id,
	        plan_name,
	        creator,
	        updator,
          create_datetime,
          update_datetime
     FROM app_store_cost
	)p1
  LEFT JOIN julive_fact.fact_market_appstore_city_share city_cost_p
    ON from_unixtime(p1.record_date ,'yyyyMMdd') = city_cost_p.pdate
    AND p1.media_type_name = city_cost_p.media_name
   
) app_store
LEFT JOIN julive_dim.dim_dsp_account_rebate dsp_account_rebate on app_store.account_id=dsp_account_rebate.dsp_account_id
          and app_store.report_date = dsp_account_rebate.rebate_timestamp
JOIN julive_dim.dim_dsp_account_history deal_dsp_account on app_store.account_id = deal_dsp_account.id  and deal_dsp_account.pdate =  from_unixtime(app_store.report_date,'yyyyMMdd') 

;

-- 第二部分 - 导入FEED数据

    INSERT OVERWRITE TABLE julive_fact.fact_market_area_report_dtl partition (pdate,source)
    SELECT 
      from_unixtime(import_feed.report_date,'yyyy-MM-dd') as report_date,
      from_unixtime(import_feed.report_date,'yyyy-MM-dd HH:mm:ss') as report_time,
      import_feed.data_from,
      if(deal_dsp_account.media_type_name like '应用市场%' ,'应用市场','feed') as media_class,
      if(deal_dsp_account.product_type_name='APP渠道', deal_dsp_account.product_type_name,
         if(deal_dsp_account.media_type_name='微信' and deal_dsp_account.product_type_name='小程序','微信小程序','M')) as device_name,
      import_feed.account_id,
      import_feed.show_num,
      import_feed.click_num,
      import_feed.bill_cost,
      coalesce(rwd.reward_actual,0) * (import_feed.bill_cost / import_feed.total_bill_cost) as reward,
      (import_feed.bill_cost - coalesce(rwd.reward_actual,0) * (import_feed.bill_cost / import_feed.total_bill_cost)) / (1 +  coalesce(dsp_account_rebate.rebate,0)) as cost,
      import_feed.download_num,
      import_feed.plan_id,
      import_feed.plan_name,
      regexp_extract(import_feed.city_name, '(.*?)(市|$)', 1)  city_name,
      null as channel_id ,
      null as channel_name  ,
      null as channel_city_name ,
      null as url_city_name ,
      null as kw_city_name ,
      if(deal_dsp_account.product_type_name='APP渠道', deal_dsp_account.app_type,'M') as app_type ,
      import_feed.creator,
      import_feed.updator,
      import_feed.create_datetime,
      import_feed.update_datetime,
      current_timestamp() as etl_time,
      from_unixtime(import_feed.report_date,'yyyyMMdd') as pdate,
      'import_feed' as source
    FROM (
        SELECT
            record_date as report_date,
            'dsp_app_data_import_data' as data_from,
            account_id ,
            0 as show_num,
    		    0 as click_num,
    		    cash_cost as bill_cost,
    		    sum(cash_cost) over (partition by record_date,account_id) as total_bill_cost,
    		    0 as download_num,
            0 as plan_id,
    		    '0' as plan_name,
    		    city_name ,
    	      creator,
    	      updator,
            create_datetime,
            update_datetime
      FROM  ods.dsp_app_data_import_data
      WHERE city_name != '全国' and city_name != '其它'
            and (media_type <> 23 or record_date < unix_timestamp('2021-05-20','yyyy-MM-dd')) -- 快手取20210520 之前的手工数据  
            and from_unixtime(dsp_app_data_import_data.record_date,'yyyy-MM-dd') >= '2021-01-01'
            --and from_unixtime(dsp_app_data_import_data.record_date,'yyyyMMdd')=${hiveconf:etl_date}
    ) import_feed
    LEFT JOIN julive_dim.dim_dsp_account_rebate dsp_account_rebate  on import_feed.account_id=dsp_account_rebate.dsp_account_id
              and import_feed.report_date = dsp_account_rebate.rebate_timestamp
    JOIN julive_dim.dim_dsp_account_history deal_dsp_account 
              on import_feed.account_id = deal_dsp_account.id  
              and from_unixtime(import_feed.report_date,'yyyyMMdd') = deal_dsp_account.pdate
    LEFT JOIN (
        select a.cur_date,
               a.dsp_account_id,
               sum(a.reward_cost) as reward_actual
        from ods.dsp_account_daily_cost a 
        where a.reward_cost <> 0  
              and from_unixtime(a.cur_date,'yyyy-MM-dd') >= '2021-01-01'
        group by a.cur_date , a.dsp_account_id
    ) rwd on import_feed.report_date = rwd.cur_date and import_feed.account_id = rwd.dsp_account_id
;
