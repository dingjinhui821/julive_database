set spark.app.name=app_market_offline_conversion_market;
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=2g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=12;

set etl_date = '${hiveconf:etlDate}';

-- set etl_yestoday = '${hiveconf:etlYestoday}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：市场线下数汇总表
--   输 入 表 ：
--         julive_dim.dim_clue_ext_base                           -- DIM-扩展线索维度表
--         julive_fact.fact_kfsclue_full_line_indi                -- 开发商线索表
--         julive_topic.topic_dwt_order_base                      -- TOPIC-DWT-订单业务指标表
--         julive_topic.topic_dws_order_city_base                 -- TOPIC-DWS-订单城市业务指标表

--   输 出 表：julive_app.app_market_offline_conversion_market
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/22 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容


with           
-- 全部线索 
clue_num_all as (
    SELECT
        channel_id,
        city_id,
        create_date as report_date,
        media_name  as media_type,
        is_short,
        org_name,
        full_type,
        null as yw_line,
        count(clue_id) as xs_cnt_all
    FROM(
        SELECT
            clue_id ,
            create_date ,
            channel_id ,
            media_name ,
            user_mobile,
            city_id,
            is_short,
            org_name,
            full_type,
            row_number() OVER(partition by user_mobile,create_date order by create_time) as r
        FROM(
            SELECT 
                clue_id,
                create_date,
                create_time,
                channel_id,
                user_mobile,
                media_name,
                is_short,
                customer_intent_city_id as city_id,
                org_name,
                full_type
            FROM julive_dim.dim_clue_ext_base a
            left join (select distinct city_id as city_id1 from julive_dim.dim_wlmq_city) b on a.city_id = b.city_id1
            where b.city_id1 is null
            union all
            select
                clue_id,
                create_date,
                create_time,
                channel_id,
                user_mobile,
                media_name,
                null as is_short,
                city_id,
                null as org_name,
                null as full_type
            from julive_fact.fact_kfsclue_full_line_indi
        )a
    )b
    WHERE r = 1
    GROUP BY
        channel_id,
        city_id,
        create_date,
        is_short,
        org_name,
        full_type,
        media_name
),

--developer_xs_cnt
developer_clue_num as 
(
    select
        channel_id,
        city_id,
        create_date as report_date,
        media_name  as media_type,
        null as is_short,
        null as org_name,
        null as full_type,
        null as yw_line,
        count(clue_id) as developer_xs_cnt
    from julive_fact.fact_kfsclue_full_line_indi
    group by 
        channel_id,
        create_date,
        city_id,
        media_name
),
order_process as 
(
    select 
      t2.customer_intent_city_id as city_id,
      t2.channel_id,
      t2.create_date as report_date,
      t2.media_name as media_type,
      t2.org_name,
      t2.full_type,
      t2.yw_line,
      t2.is_short,
      sum(t1.distribute_probs) as probs,
      sum(t1.clue_score) as xs_score,
      sum(t1.first_call_duration/60.0)  as first_call_duration,
      sum(t1.intent_low_num) as intent_low_num,
      sum(if(t1.first_call_duration > 0,1,0)) as first_call_duration_num,
      sum(if(t2.source in (9,10),t1.distribute_num,0)) as `400_sh_cnt` 
   from julive_topic.topic_dwt_order_base t1
   join (
       select 
           clue_id,
           customer_intent_city_id,
           create_date,
           media_name,
           org_name,
           full_type,
           channel_id,
           yw_line,
           is_short,
           source
       from julive_dim.dim_clue_ext_base a 
       left join (select distinct city_id from julive_dim.dim_wlmq_city) b on a.city_id = b.city_id
       where b.city_id is null
   ) t2 on t1.clue_id = t2.clue_id
   group by 
      t2.customer_intent_city_id,
      t2.channel_id,
      t2.create_date,
      t2.media_name,
      t2.org_name,
      t2.full_type,
      t2.yw_line,
      t2.is_short
),

time_process as (
   select 
      t1.city_id,
      t1.report_date,
      t2.media_name as media_type,
      t2.org_name,
      t2.full_type,
      t2.channel_id,
      t2.yw_line,
      t2.is_short,
      sum(t1.clue_num) as xs_cnt,
      sum(t1.clue_num_400) as `400_xs_cnt`,
      sum(t1.distribute_num) as sh_cnt,
      sum(if(t1.call_duration>0 and t1.report_date = t3.distribute_date,1,0)) as jietong_sh_day,
      sum(t1.see_num)  as dk_cnt,
      sum(t1.see_num_online) as online_dk_cnt,
      sum(t1.subscribe_num) as rg_cnt,
      sum(t1.subscribe_coop_num) as rg_cnt_net,
      sum(t1.subscribe_contains_cancel_ext_income) as rengou_yingshou,
      sum(t1.subscribe_contains_ext_income) as rengou_yingshou_net,
      sum(t1.sign_num) as qy_cnt,
      sum(t1.sign_income_orig) as qianyue_yingshou
   from julive_topic.topic_dws_order_city_base t1
   join (
       select 
           clue_id,
           media_name,
           org_name,
           full_type,
           channel_id,
           yw_line,
           is_short
       from julive_dim.dim_clue_ext_base a 
       left join (select distinct city_id from julive_dim.dim_wlmq_city) b on a.city_id = b.city_id
       where b.city_id is null
   ) t2 on t1.clue_id = t2.clue_id
   left join julive_topic.topic_dwt_order_base t3 on t1.clue_id = t3.clue_id
   group by 
      t1.city_id,
      t1.report_date,
      t2.media_name,
      t2.org_name,
      t2.full_type,
      t2.channel_id,
      t2.yw_line,
      t2.is_short
)




insert overwrite table julive_app.app_market_offline_conversion_market
select 
coalesce(t1.report_date,t2.report_date,t3.report_date,t4.report_date) as report_date,
coalesce(t1.city_id,t2.city_id,t3.city_id,t4.city_id) as city_id,
coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id) as channel_id,
coalesce(t1.media_type,t2.media_type,t3.media_type,t4.media_type) as media_type,
coalesce(t1.is_short,t2.is_short,t3.is_short,t4.is_short) as is_short,
coalesce(t1.org_name,t2.org_name,t3.org_name,t4.org_name) as org_name,
coalesce(t1.full_type,t2.full_type,t3.full_type,t4.full_type) as full_type,
coalesce(t1.yw_line,t2.yw_line,t3.yw_line,t4.yw_line) as yw_line,
t2.rengou_yingshou,  
t2.rengou_yingshou_net, 
t2.qianyue_yingshou,
t2.xs_cnt,
t2.sh_cnt,
t2.dk_cnt,
t2.rg_cnt,
t2.qy_cnt,
t1.probs,  
t2.`400_xs_cnt`,
t4.developer_xs_cnt,
t2.jietong_sh_day,
t1.xs_score,
t1.first_call_duration,
t1.first_call_duration_num ,
t1.intent_low_num,
t3.xs_cnt_all,
t2.online_dk_cnt, 
t2.rg_cnt_net,
t1.`400_sh_cnt`
from order_process t1
full join time_process t2
on nvl(t1.city_id,0) = nvl(t2.city_id,0)
and  t1.report_date = nvl(t2.report_date,'')
and  nvl(t1.media_type,'') = nvl(t2.media_type,'')
and  nvl(t1.org_name,'') = nvl(t2.org_name,'')
and  nvl(t1.full_type,'') = nvl(t2.full_type,'')
and  nvl(t1.channel_id,0) = nvl(t2.channel_id,0)
and  nvl(t1.yw_line,'') = nvl(t2.yw_line,'')
and  nvl(t1.is_short,'') = nvl(t2.is_short,'')
full join clue_num_all t3
on coalesce(t1.city_id,t2.city_id,0) = nvl(t3.city_id,0)
and  coalesce(t1.report_date,t2.report_date) = nvl(t3.report_date,'')
and  coalesce(t1.media_type,t2.media_type,'') = nvl(t3.media_type,'')
and  coalesce(t1.org_name,t2.org_name,'') = nvl(t3.org_name,'')
and  coalesce(t1.full_type,t2.full_type,'') = nvl(t3.full_type,'')
and  coalesce(t1.channel_id,t2.channel_id,0) = nvl(t3.channel_id,0)
and  coalesce(t1.yw_line,t2.yw_line,'') = nvl(t3.yw_line,'')
and  coalesce(t1.is_short,t2.is_short,'') = nvl(t3.is_short,'')
full join developer_clue_num t4
on coalesce(t1.city_id,t2.city_id,t3.city_id,0) = nvl(t4.city_id,0)
and  coalesce(t1.report_date,t2.report_date,t3.report_date) = nvl(t4.report_date,'')
and  coalesce(t1.media_type,t2.media_type,t3.media_type,'') = nvl(t4.media_type,'')
and  coalesce(t1.org_name,t2.org_name,t3.org_name,'') = nvl(t4.org_name,'')
and  coalesce(t1.full_type,t2.full_type,t3.full_type,'') = nvl(t4.full_type,'')
and  coalesce(t1.channel_id,t2.channel_id,t3.channel_id,0) = nvl(t4.channel_id,0)
and  coalesce(t1.yw_line,t2.yw_line,t3.yw_line,'') = nvl(t4.yw_line,'')
and  coalesce(t1.is_short,t2.is_short,t3.is_short,'') = nvl(t4.is_short,'')

;






drop view dwd.offline_conversion_market;

create view dwd.offline_conversion_market as
select 
    t1.report_date as `date`,
    t2.city_name as city,
    t2.module_name as product_type,
    t1.media_type as media_type,
    t2.device_name as device_type,
    t2.app_type_name as app_type,
    t1.rengou_yingshou as rengou_yingshou,
    t1.rengou_yingshou_net as rengou_yingshou_net,
    t1.qianyue_yingshou as qianyue_yingshou,
    t1.xs_cnt as xs_cnt,
    t1.sh_cnt as sh_cnt,
    t1.dk_cnt as dk_cnt,
    t1.rg_cnt as rg_cnt,
    t1.qy_cnt as qy_cnt,
    t2.channel_id as channel_id,
    t2.channel_name as channel_name,
    t1.probs as probs,
    t1.`400_xs_cnt` as `400_xs_cnt`,
    t1.developer_xs_cnt as developer_xs_cnt,
    t1.jietong_sh_day as jietong_sh_day,
    t1.xs_score as xs_score,
    t1.first_call_duration as first_call_duration,
    t1.intent_low_num as intent_low_num,
    t1.xs_cnt_all as xs_cnt_all,
    t1.first_call_duration_num as first_call_duration_num,
    t1.is_short as is_short,
    online_dk_cnt as online_dk_cnt,
    t1.rg_cnt_net as rg_cnt_net,
    t1.`400_sh_cnt` as `400_sh_cnt`,
    t1.org_name,
    t1.full_type,
    t1.yw_line
from julive_app.app_market_offline_conversion_market t1
join julive_dim.dim_channel_info t2 on t1.channel_id = t2.channel_id
;