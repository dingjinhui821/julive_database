

set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=4g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=12;

set hive.execution.engine=spark;

set spark.app.name=app_market_app_process;



with unit_id_name AS
(
    SELECT unit_name,
           max(unit_id) as unit_id
    FROM ods.dsp_creative_report
    WHERE --dsp_account_id = 551
       unit_name IS NOT NULL AND from_unixtime(report_date,'yyyy-MM')>= '2020-12'
    group by unit_name
),
appinstall_list as 
(    select t.global_id,
           cast(t.install_date_time as string) as install_date_time,
           t.`$utm_source` as utm_source,
           t.select_city,
           t.adgroup_name,
           t.channel,
           coalesce(t.aid,t.`$utm_campaign`,t.plan_id,cast(unit_id_name.unit_id as string)) as aid, 
           coalesce(cid,`$utm_content`) as cid,
           row_number() over (partition by t.global_id order by t.install_date_time desc) max_rn
    from dwd.dwd_appinstall_channel_match_by_global  t
    left join unit_id_name on t.adgroup_name = unit_id_name.unit_name
),
appinstall_range as 
(  select 
          global_id,
          install_date_time,
          lead(install_date_time) over (partition by global_id order by install_date_time asc) as after_install_date_time,
          utm_source,
          channel,
          select_city,
          aid,
          cid
    from 
   ( select global_id,install_date_time,utm_source,channel,select_city,aid,cid from appinstall_list 
    union all 
    select global_id,'9999-12-31' as install_date_time,utm_source,channel,select_city,aid,cid from appinstall_list 
     where max_rn = 1
   )  as list
),

global_julive_list as 
(   select 
        global_julive.*,
        row_number() over(partition by julive_id order by create_time asc) min_rn,
        row_number() over(partition by julive_id order by create_time desc) max_rn
    from(
        select julive_id ,
                global_id ,
                min(create_time) as create_time 
        from julive_fact.global_julive_id_leave_phone
        group by global_id,julive_id
    ) as global_julive
) 

,global_julive_range as 
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
     select julive_id,global_id, '9999-12-31' as leave_time from global_julive_list where max_rn = 1 -- 上届
    ) as list
)

   select 
         t.id as order_id,
         t.create_datetime as create_order_datetime,
         from_unixtime(t.create_datetime,'yyyy-MM-dd') as create_order_date,
         t.channel_id,
         t.user_id,
         t2.utm_source,
         t2.select_city,
         t2.channel,
         t2.install_date_time,
         t2.aid,
         t2.cid
    from ods.yw_order t 
    left join global_julive_range t1  on cast(t.user_id as string) = t1.julive_id
    left join appinstall_range t2 on t1.global_id = t2.global_id
    where to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) >= to_date(t1.leave_time) 
          and to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) < to_date(t1.after_leave_time)
          and to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) >= to_date(t2.install_date_time) 
          and to_date(from_unixtime(t.create_datetime,'yyyy-MM-dd')) < to_date(t2.after_install_date_time)
;


-- 关联留电信息     规则1、如果存在order_create_time之前的留电，取order_create_time之前最大留电时间对应global_id 
--                   2、否则取最大留电时间对应global_id
-- 关联app安装信息  规则1、如果存在order_create_time之前的留电，取order_create_time之前最大安装时间对应纪录
--                   2、否则为空
