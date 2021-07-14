set spark.app.name=fact_market_order_rel_appinstall;
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=4g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=6;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

set etl_date = '${hiveconf:etlDate}';
-- set etl_yestoday = '${hiveconf:etlYestoday}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：订单关联AppInstall逻辑
--   输 入 表 ：
--         ods.yw_order                                           -- 订单信息表
--         ods.dsp_creative_report                                -- FEED创意报告 - 非当天依赖表
--         dwd.dwd_appinstall_channel_match_by_global             -- AppInstall埋点信息
--         julive_fact.global_julive_id_leave_phone               -- 留电埋点信息 与 AppInstall埋点信息在同一个人物

--   输 出 表：julive_fact.fact_market_order_rel_appinstall
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/05/25 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

--   功能： 订单信息关联AppInstall埋点信息
--   规则：
--     关联留电信息  1、如果存在order_create_time之前的留电，取order_create_time之前最大留电时间对应global_id 
--                 2、否则取最大留电时间对应global_id
--     关联安装信息  1、如果存在order_create_time之前的留电，取order_create_time之前最大安装时间对应纪录
--                 2、否则不取数据


with unit_id_name AS
(
    SELECT unit_name,
           max(unit_id) as unit_id
    FROM ods.dsp_creative_report
    WHERE --dsp_account_id = 551
       unit_name IS NOT NULL AND from_unixtime(report_date,'yyyy-MM')>= '2020-12'
    group by unit_name
),
data_range as 
(
     select 
          t.id,
          t.create_datetime,
          t.channel_id,
          t.user_id
     from ods.yw_order t
     left join julive_dim.dim_channel_info channel on t.channel_id = channel.channel_id
     left join (
         SELECT DISTINCT order_id  FROM ods.yw_order_attr WHERE product_id = 2 AND comjia_platform_id IN (101,201) 
     ) attr on t.id = attr.order_id
     where from_unixtime(create_datetime,'yyyyMMdd') = ${hiveconf:etl_date}
           and ( channel.device_name = 'APP' 
               or attr.order_id is not null)  
),

global_julive_list as 
(   select 
        global_julive.*,
        row_number() over(partition by julive_id order by create_time asc) min_rn,
        row_number() over(partition by julive_id order by create_time desc) max_rn
    from(
        select  julive_id ,
                global_id ,
                min(create_time) as create_time 
        from julive_fact.global_julive_id_leave_phone t
        left semi join 
        (
            select cast(user_id as string) as user_id from data_range
        ) t1 on t.julive_id = t1.user_id
        group by global_id,julive_id
    ) as global_julive
) ,
-- hbase mapping
-- global_hbase_2_hive as 
-- (
--         select 
--             t.julive_id ,
--             max(global_id) as global_id,
--             1 as min_rn
--         from (
--            select vt.julive_id,
--               global_id
--            from julive_fact.global_id_hbase_2_hive t 
--            lateral view explode(split(julive_id,'\\|')) vt as julive_id
--         ) t
--         left semi join 
--         (
--             select cast(user_id as string) as user_id from data_range
--         ) t1 on t.julive_id = t1.user_id
--         group by t.julive_id
-- ),
-- 

global_julive_range as 
(    select 
          julive_id,
          leave_time,
          lead(leave_time) over (partition by julive_id order by leave_time asc) as after_leave_time,
          global_id
    from 
   ( select julive_id,global_id, '1900-01-01' as leave_time from global_julive_list where min_rn = 1 -- 下届
     union all
     select julive_id,global_id, create_time as leave_time from global_julive_list  -- 实际值
     union all
     select julive_id,global_id, '3000-12-31' as leave_time from global_julive_list where max_rn = 1 -- 上届
     -- 考虑效率  先不取hbase mapping数据
     -- union all
     -- select julive_id,global_id, '1800-01-01' as leave_time from global_hbase_2_hive where min_rn = 1
     -- union all
     -- select julive_id,global_id, '4000-12-31' as leave_time from global_hbase_2_hive
     
   ) as list
),
appinstall_list as 
(    select t.global_id,
           cast(t.install_date_time as string) as install_date_time,
           t.`$utm_source` as utm_source,
           t.select_city,
           t.`$city`,
           t.product_id,
           t.adgroup_name,
           t.channel,
           coalesce(t.aid,t.`$utm_campaign`,t.plan_id,cast(unit_id_name.unit_id as string)) as aid, 
           coalesce(cid,`$utm_content`) as cid,
           row_number() over (partition by t.global_id order by t.install_date_time desc) max_rn
    from dwd.dwd_appinstall_channel_match_by_global  t
    left join unit_id_name on t.adgroup_name = unit_id_name.unit_name
    left semi join (
       select global_id from global_julive_list
    ) t2 on t.global_id = t2.global_id
),
appinstall_range as 
(  select 
          global_id,
          install_date_time,
          lead(install_date_time) over (partition by global_id order by install_date_time asc) as after_install_date_time,
          utm_source,
          channel,
          select_city,
          `$city`,
          product_id,
          aid,
          cid
    from 
   ( 
       select global_id,'1900-01-01' as install_date_time, null as utm_source,null as channel, null as select_city,
           null as `$city`,null as product_id,null as aid,null as cid from appinstall_list 
       where max_rn = 1
       union all
   
       select global_id,install_date_time,utm_source,channel,select_city,`$city`,product_id,aid,cid from appinstall_list 
       union all 
       select global_id,'9999-12-31' as install_date_time,utm_source,channel,select_city,`$city`,product_id,aid,cid from appinstall_list 
       where max_rn = 1
   )  as list
)




   insert overwrite TABLE julive_fact.fact_market_order_rel_appinstall partition (pdate)
   select 
         t1.id as order_id,
         from_unixtime(t1.create_datetime,'yyyy-MM-dd') as create_order_date,
         from_unixtime(t1.create_datetime,'yyyy-MM-dd HH:mm:ss') as create_order_time,
         t1.channel_id,
         t1.channel_name,
         t1.user_id,
         t1.global_id,
         t2.utm_source,
         t2.select_city,
         t2.`$city`,
         t2.product_id,
         t2.channel,
         if(t2.install_date_time = '1900-01-01' , null , t2.install_date_time) as install_date_time,
         t2.aid,
         t2.cid,
         current_timestamp() as etl_time,
         from_unixtime(t1.create_datetime,'yyyyMMdd') as pdate
    from 
    (
        select 
            t.id,
            t.create_datetime,
            t.channel_id,
            t.channel_name,
            t.user_id,
            t1.global_id,
            to_date(t1.leave_time) as leave_time
        from 
        (
            select 
               data_range.*,
               split(channel.channel_name,'-')[4] as channel_name
            from data_range
            left join julive_dim.dim_channel_info  channel on data_range.channel_id = channel.channel_id
        ) t
        left join global_julive_range t1  on cast(t.user_id as string) = t1.julive_id
        where (to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) >= to_date(t1.leave_time) 
              and to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) < to_date(t1.after_leave_time)
              ) or t1.julive_id is null
    ) as t1
    left join appinstall_range t2 on t1.global_id = t2.global_id
    where
          (( to_date(from_unixtime(t1.create_datetime,'yyyy-MM-dd')) >= to_date(t2.install_date_time) 
            and to_date(from_unixtime(t1.create_datetime,'yyyy-MM-dd')) < to_date(t2.after_install_date_time)
          ) ) or t2.global_id is null
          
;
