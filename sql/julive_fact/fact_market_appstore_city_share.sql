set hive.execution.engine=spark;
set spark.app.name=fact_market_appstore_city_share;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=3g;
set spark.executor.instances=5;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

set etl_date = '${hiveconf:etlDate}';
-- set etl_yestoday = '${hiveconf:etlYestoday}'; 
-- set etl_date = '20210101';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：媒体报告-应用商城按城市分摊比例
--   输 入 表 ：
--         ods.yw_order                                  -- 订单表
--         julive_fact.fact_market_order_rel_appinstall  -- 订单 激活对应关系表
--         julive_dim.dim_channel_info                   -- 渠道配置表
--         julive_dim.dim_city                           -- 城市配置表
--         julive_fact.fact_market_area_report_dtl       -- 媒体报告 - 取历史SEM城市数据，非当天依赖
--   输 出 表：julive_fact.fact_market_appstore_city_share
-- 
--   创 建 者： 薛理  15996981324
--   创建日期： 2021/06/01 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

--   对于应用商店(应用市场（苹果）、应用市场（安卓）)媒体数据，投放不区分地域，需要拆分到地市处理
--   拆分算法：
--      1、 计算 SEM 每天各城市 往前7个月 平均上户消耗
--      2、 计算 各媒体 每天 各城市 上户数量
--      3、 计算比率 per = (上户数量 * 平均上户消耗) /SUM(上户数量 * 平均上户消耗)




with sem_sh_city_list as (
    -- 按日期 渠道id 设备类型 分组 计算城市分摊比例
    select
        tab.report_date,
        tab.channel_id,
        tab.device_type,
        tab.city_name,
        tab.sh_cnt,
        sum(tab.sh_cnt) over (partition by tab.report_date,tab.channel_id,tab.device_type) as total_sh_cnt
    from(    
        SELECT
                t.report_date,
                channel.device_name as device_type,
                t.channel_id,
                city.city_name,
                count(t.id) as  sh_cnt
        from (
            select 
                t.id,
                from_unixtime(distribute_datetime,'yyyy-MM-dd') as report_date,
                t.device_type,
                t.channel_id,
                t.customer_intent_city
            from ods.yw_order t
            where t.is_distribute = 1
                  and from_unixtime(distribute_datetime,'yyyy-MM-dd') 
                      between  date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-7)
                      and date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),0)
                  and t.city_id not in(select city_id from julive_dim.dim_wlmq_city )
        )  t
        left join julive_dim.dim_channel_info channel on t.channel_id = channel.channel_id
        left join julive_dim.dim_city city on t.customer_intent_city = city.city_id
        where channel.module_name = 'SEM'
        group by 
            report_date,
            channel.device_name,
            t.channel_id,
            city.city_name
            
    ) as tab
),
sem_sh_city as 
(
    select 
       t.*
    from sem_sh_city_list  t
    where report_date between  date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-7)
                      and date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-1)
          and t.city_name in 
          (
              select 
                  city_name 
              from sem_sh_city_list 
              where report_date = date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),0)
          ) 
),

city_sh_cost as 
(
    select 
        city_cost.report_date,
        city_cost.city_name as city,
        city_cost.cost as sem_cost,
        city_sh.sh_cnt as sem_sh_cnt,
        city_cost.cost/city_sh.sh_cnt as sem_sh_cost
    from(
        select 
            from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd') as report_date,
            coalesce(sem.city_name, cost.city_name ) as city_name,
            sum(cost.cost * if(sem.total_sh_cnt = 0 or sem.total_sh_cnt is null ,1 ,(sem.sh_cnt / sem.total_sh_cnt))) as cost
        from(
            select 
                t.report_date,
                t.channel_id,
                channel.city_name,
                t.device_name as device_type,
                t.cost
            from  julive_fact.fact_market_area_report_dtl t
            left join julive_dim.dim_channel_info channel on t.channel_id = channel.channel_id 
            where t.source = 'SEM' and t.device_name != 'APP渠道'
                  -- and t.device_name in ('PC','M')
                  and   from_unixtime(unix_timestamp(t.pdate,'yyyyMMdd'),'yyyy-MM-dd')
                           between date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-7)
                              and date_add(from_unixtime(unix_timestamp( ${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-1)
                  
        ) cost 
        left join sem_sh_city_list sem 
             on cost.report_date = sem.report_date
             and cost.channel_id = sem.channel_id  
             and cost.device_type = sem.device_type
        group by coalesce(sem.city_name, cost.city_name ) 
    ) city_cost             
    left join  
    (
       select 
           city_name,
           sum(sh_cnt) as sh_cnt
       from sem_sh_city
       group by city_name
    ) city_sh on city_cost.city_name = city_sh.city_name
) ,
-- 2、 计算 各媒体 每天 各城市 上户数量,

media_sh_day as 
(
    SELECT
      from_unixtime(distribute_datetime,'yyyy-MM-dd') as record_date,
      coalesce(utm_channel.media_type,channel.media_name) as  media_type_name,
      city.city_name as city ,
      count(id) as appstore_sh_cnt 
    from ods.yw_order t 
    left join julive_fact.fact_market_order_rel_appinstall t1 on t.id = cast(t1.order_id as int)
    left join julive_dim.dim_channel_info channel on t.channel_id = channel.channel_id
    left join julive_dim.dim_city city on t.customer_intent_city = city.city_id
    left join (
        select 
            t1.utm_source,
            max(t1.media_name) as media_type
        from julive_dim.dim_channel_info  t1 
        where t1.media_name is not null 
        group by t1.utm_source
    ) utm_channel on coalesce(t1.utm_source , t1.channel ,t1.channel_name) = utm_channel.utm_source
    WHERE channel.module_name = 'APP渠道' and coalesce(utm_channel.media_type,channel.media_name) like '应用市场%'
     and from_unixtime(distribute_datetime,'yyyyMMdd') = ${hiveconf:etl_date}
     and is_distribute = 1
    GROUP BY
      from_unixtime(distribute_datetime,'yyyy-MM-dd') ,
      city.city_name ,
      coalesce(utm_channel.media_type,channel.media_name)
)

--      3、 计算比率 per = (上户数量 * 平均上户消耗) /SUM(上户数量 * 平均上户消耗)
INSERT OVERWRITE TABLE julive_fact.fact_market_appstore_city_share partition (pdate)
select
    report_date,
    city as city_name,
    media_type_name as media_name,
    sem_cost,
    sem_sh_cnt,
    sem_sh_cost,
    appstore_sh_cnt,
    (mul_cost/mul_cost_total) as city_rate,
    current_timestamp() as etl_time,
    date_format(report_date,'yyyyMMdd') as pdate
from(    
    SELECT 
         city_sh_cost.report_date,
         city_sh_cost.city,
         media_sh_day.media_type_name,
         city_sh_cost.sem_cost,
         city_sh_cost.sem_sh_cnt,
         city_sh_cost.sem_sh_cost,
         media_sh_day.appstore_sh_cnt,
         city_sh_cost.sem_sh_cost * media_sh_day.appstore_sh_cnt as  mul_cost,
         sum(city_sh_cost.sem_sh_cost * media_sh_day.appstore_sh_cnt) over (partition by city_sh_cost.report_date,media_sh_day.media_type_name) as mul_cost_total
     FROM city_sh_cost 
     join media_sh_day  on city_sh_cost.city = media_sh_day.city
	        and city_sh_cost.report_date = media_sh_day.record_date
     where city_sh_cost.sem_sh_cost  > 0 and media_sh_day.appstore_sh_cnt > 0 
) media_city_cost_p
;