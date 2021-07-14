

set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=5g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=12;
set spark.yarn.queue=etl;
set hive.execution.engine=spark;
set spark.app.name=app_market_app_process;


set etl_date = '${hiveconf:etlDate}';
set etl_yestoday = '${hiveconf:etlYestoday}'; 
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 


--   功能描述：APP全流程底表
--   输 入 表 ：
--         julive_dim.dim_clue_ext_base                           -- 线索维度扩展表
--         dwd.dwd_appinstall_channel_match_by_global             -- AppInstall埋点信息
--         julive_topic.topic_dws_order_city_base                 -- TOPIC-DWS-订单城市业务指标表
--         julive_topic.topic_dwt_order_base                      -- TOPIC-DWT-订单业务指标表
--         julive_fact.fact_event_sign_up_base_dtl                -- 注册日志信息表
--         julive_fact.fact_market_area_report_dtl                -- 地域-展点销信息表

--   输 出 表：julive_app.app_market_app_process
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/17 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期：           修改人   修改内容
--   2021/06/25         薛理     增加注册数指标

 
    with jihuo  as  (
       SELECT 
            to_date(t.install_date_time) as report_date,
            t.city as city_name,
            t.utm_source as app_source,
            t.app_type,
            cj_agency.channel_type_name as media_type,
            sum(jh_cnt) as jh_cnt,
           sum(jh_cnt_xin) as jh_cnt_xin
        FROM 
        (   
            SELECT 
                t.install_date_time,
                if(t.city is not null or t.city != '',city,dim_city.city_name) as city,
                utm_source,
                app_type,
                jh_cnt,
                jh_cnt_xin
            FROM 
            (
                SELECT
                    to_date(t.install_date_time) as install_date_time,
                    if(`$utm_source` is not null and `$utm_source`!='',`$utm_source`,channel) as utm_source,
                    t.select_city,
                    t.`$city` as city,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL)) AS app_type,
                  count(DISTINCT global_id) AS jh_cnt,
                   count(DISTINCT if(`$is_first_day` = 1, global_id,null)) as jh_cnt_xin
                FROM dwd.dwd_appinstall_channel_match_by_global t
                -- WHERE pdate >= '20210501'
                group by to_date(t.install_date_time),
                    if(`$utm_source` is not null and `$utm_source`!='',`$utm_source`,channel),
                    t.select_city,
                    t.`$city`,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL))
                
            ) t
            LEFT JOIN julive_dim.dim_city ON t.select_city = cast(dim_city.city_id as string)
        ) t
        LEFT JOIN 
        (
            SELECT DISTINCT
                if(app_id = 101,'安卓',
                if(app_id = 201,'苹果',NULL)) AS app_type,
                utm_source,
                channel_type_name
            FROM ods.cj_agency
        )cj_agency ON t.utm_source = cj_agency.utm_source AND t.app_type = cj_agency.app_type
        group by to_date(t.install_date_time),
            t.city,
            t.utm_source,
            t.app_type,
            cj_agency.channel_type_name
    ) ,
    
    
    register_uv as (
        SELECT 
            t.install_date as report_date,
            t.city as city_name,
            t.utm_source as app_source,
            t.app_type,
            cj_agency.channel_type_name as media_type,
            sum(register_cnt_uv) as register_cnt_uv
        FROM 
        (   
            SELECT 
                t.install_date,
                if(t.city is not null or t.city != '',city,dim_city.city_name) as city,
                utm_source,
                app_type,
                register_cnt_uv
            FROM 
            (
               SELECT
                    to_date(install_datetime) as install_date,
                    if(utm_source is not null and utm_source!='',utm_source,channel) as utm_source,
                    t.select_city,
                    t.`$city` as city,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL)) AS app_type,
                  count(DISTINCT global_id) AS register_cnt_uv
                FROM julive_fact.fact_event_sign_up_base_dtl t
                where substr(product_id,1,3) in ('101','201') and install_datetime is not null
                group by to_date(install_datetime),
                    if(utm_source is not null and utm_source!='',utm_source,channel) ,
                    t.select_city,
                    t.`$city`,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL))
                
            ) t
            LEFT JOIN julive_dim.dim_city ON t.select_city = cast(dim_city.city_id as string)
        ) t
        LEFT JOIN 
        (
            SELECT DISTINCT
                if(app_id = 101,'安卓',
                if(app_id = 201,'苹果',NULL)) AS app_type,
                utm_source,
                channel_type_name
            FROM ods.cj_agency
        )cj_agency ON t.utm_source = cj_agency.utm_source AND t.app_type = cj_agency.app_type
        group by t.install_date,
            t.city,
            t.utm_source,
            t.app_type,
            cj_agency.channel_type_name
    ),
    register_time as (
        SELECT 
            t.create_date as report_date,
            t.city as city_name,
            t.utm_source as app_source,
            t.app_type,
            cj_agency.channel_type_name as media_type,
            sum(register_cnt_time) as register_cnt_time
        FROM 
        (   
            SELECT 
                t.create_date,
                if(t.city is not null or t.city != '',city,dim_city.city_name) as city,
                utm_source,
                app_type,
                register_cnt_time
            FROM 
            (
               SELECT
                    from_unixtime(t.create_datetime,'yyyy-MM-dd') as create_date,
                    if(utm_source is not null and utm_source!='',utm_source,channel) as utm_source,
                    t.select_city,
                    t.`$city` as city,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL)) AS app_type,
                    count(DISTINCT global_id) AS register_cnt_time
                FROM julive_fact.fact_event_sign_up_base_dtl t
                where substr(product_id,1,3) in ('101','201')
                group by from_unixtime(t.create_datetime,'yyyy-MM-dd'),
                    if(utm_source is not null and utm_source!='',utm_source,channel) ,
                    t.select_city,
                    t.`$city`,
                    if(substr(product_id,1,3) = '101','安卓', if(substr(product_id,1,3) = '201','苹果',NULL))
                
            ) t
            LEFT JOIN julive_dim.dim_city ON t.select_city = cast(dim_city.city_id as string)
        ) t
        LEFT JOIN 
        (
            SELECT DISTINCT
                if(app_id = 101,'安卓',
                if(app_id = 201,'苹果',NULL)) AS app_type,
                utm_source,
                channel_type_name
            FROM ods.cj_agency
        )cj_agency ON t.utm_source = cj_agency.utm_source AND t.app_type = cj_agency.app_type
        group by t.create_date,
            t.city,
            t.utm_source,
            t.app_type,
            cj_agency.channel_type_name
    ),

    -- 时间维度数据
    time_process as
    (
        select 
            t1.report_date,
            t2.media_name as media_type,
            t2.app_type_name as app_type,
            t2.app_source,
            t2.customer_intent_city_name as city_name,
            sum(t1.clue_num) as xs_cnt,
            sum(if(t1.report_date = t2.install_date,t1.clue_num, 0)) as xs_cnt_sameday,
            sum(t1.distribute_num) as sh_cnt,
            sum(if(t1.report_date = t2.create_date and t2.create_date = t2.install_date, t1.distribute_num,0)) as sh_cnt_sameday,
            sum(if(t2.from_source = 1 and t2.is_distribute = 1,t1.see_num,0)) as dk_cnt,
            sum(if(t2.from_source = 1 ,t1.subscribe_num,0)) as rg_cnt,
            sum(if(t2.from_source = 1 ,t1.subscribe_contains_cancel_ext_income,0)) as rengou_yingshou
        from julive_topic.topic_dws_order_city_base t1
        join (
           select 
               a.clue_id,
               a.create_date,
               a.install_date,
               a.app_type_name,
               a.app_source,
               a.customer_intent_city_name,
               a.media_name,
               a.from_source,
               a.is_distribute,
               a.device_name
           from julive_dim.dim_clue_ext_base a 
           left join (select distinct city_id from julive_dim.dim_wlmq_city) b on a.city_id = b.city_id
           where b.city_id is null
        ) t2 on t1.clue_id = t2.clue_id
        where t2.device_name ='APP' and t2.is_distribute != 16
        group by 
            t1.report_date,
            t2.media_name,
            t2.app_type_name,
            t2.app_source,
            t2.customer_intent_city_name
           
    ),
    -- 用户维度
    uv_process as
    (   
        SELECT
            t2.install_date as report_date,
            t2.install_city_name as city_name,
            t2.app_source,
            t2.install_app_type_name as app_type,
            t2.media_name as media_type,
            sum(if(t1.create_date>=t1.install_date,t1.clue_num,0)) as xs_cnt,
            sum(if(datediff(t1.create_date,t1.install_date) = 0,t1.clue_num,0 )) as xs_cnt_0d,
            sum(if(datediff(t1.create_date,t1.install_date) between 0 and 3 ,t1.clue_num,0 )) as xs_cnt_3d,
            sum(if(datediff(t1.create_date,t1.install_date) between 0 and 7 ,t1.clue_num,0 )) as xs_cnt_7d,
            sum(if(datediff(t1.create_date,t1.install_date) between 0 and 14,t1.clue_num,0 )) as xs_cnt_14d,
            sum(if(datediff(t1.create_date,t1.install_date) between 0 and 30,t1.clue_num,0 )) as xs_cnt_30d,
            sum(if(t1.distribute_date>=t1.install_date, t1.distribute_num,0)) as sh_cnt,
            sum(if(datediff(t1.distribute_date,t1.install_date)=0 , t1.distribute_num,0 )) as sh_cnt_0d,
            sum(if(datediff(t1.distribute_date,t1.install_date) between 0 and 3 ,t1.distribute_num,0 )) as sh_cnt_3d,
            sum(if(datediff(t1.distribute_date,t1.install_date) between 0 and 7 ,t1.distribute_num,0 )) as sh_cnt_7d,
            sum(if(datediff(t1.distribute_date,t1.install_date) between 0 and 14,t1.distribute_num,0 )) as sh_cnt_14d,
            sum(if(t2.from_source = 1 and t1.first_see_date>=t1.install_date and t1.see_num > 0 ,1 ,0)) as dk_cnt,
            sum(if(t2.from_source = 1 and t1.first_subscribe_date>=t1.install_date and t1.subscribe_num > 0, 1,0)) as rg_cnt,
            sum(if(t2.from_source = 1 and t1.first_subscribe_date>=t1.install_date,t1.subscribe_contains_cancel_ext_income,0.0)) as rengou_yingshou
       FROM julive_topic.topic_dwt_order_base t1
      join julive_dim.dim_clue_ext_base t2 on t1.clue_id = t2.clue_id
     where t2.device_name ='APP' and t2.is_distribute != 16
         and t2.install_date is not null
       group by t2.install_date,
            t2.install_city_name,
            t2.app_source,
            t2.install_app_type_name,
            t2.media_name
    ),
    
    -- 订单维度
    order_process as 
    (
    
      SELECT
          t2.create_date as report_date,
          t2.media_name as media_type,
          t2.app_type_name as app_type,
          t2.app_source,
          t2.customer_intent_city_name as city_name,
          sum(t1.distribute_num) as sh_cnt_order,
          sum(if(datediff(t1.distribute_date,t1.create_date)=0,t1.distribute_num,null)) as sh_cnt_order_0d,
          sum(if(datediff(t1.distribute_date,t1.create_date)<=3,t1.distribute_num,null)) as sh_cnt_order_3d,
          sum(if(datediff(t1.distribute_date,t1.create_date)<=7,t1.distribute_num,null)) as sh_cnt_order_7d,
          sum(if(datediff(t1.distribute_date,t1.create_date)<=14,t1.distribute_num,null)) as sh_cnt_order_14d,
          sum(if(datediff(t1.distribute_date,t1.create_date)<=30,t1.distribute_num,null)) as sh_cnt_order_30d,
          sum(if(t2.from_source=1,t1.see_num,0)) as dk_cnt_order,
          sum(if(t2.from_source=1 and datediff(t1.first_see_date,t1.distribute_date)<=7 and t1.see_num > 0,1,0)) as dk_cnt_order_7d,
          sum(if(t2.from_source=1 and datediff(t1.first_see_date,t1.distribute_date)<=14 and t1.see_num > 0,1,0)) as dk_cnt_order_14d,
          sum(if(t2.from_source=1 and datediff(t1.first_see_date,t1.distribute_date)<=30 and t1.see_num > 0,1,0)) as dk_cnt_order_30d,
          sum(if(t2.from_source=1,t1.subscribe_num,0)) as rg_cnt_order,
          sum(if(t2.from_source=1 and datediff(t1.first_subscribe_date,t1.first_see_date)<=7 and t1.subscribe_num >0,1 ,0)) as rg_cnt_order_7d,
          sum(if(t2.from_source=1 and datediff(t1.first_subscribe_date,t1.first_see_date)<=14 and t1.subscribe_num >0 ,1 ,0)) as rg_cnt_order_14d,
          sum(if(t2.from_source=1 and datediff(t1.first_subscribe_date,t1.first_see_date)<=30 and t1.subscribe_num >0, 1,0)) as rg_cnt_order_30d
      FROM julive_topic.topic_dwt_order_base t1
      join (
         select 
             a.clue_id,
             a.create_date,
             a.media_name,
             a.app_type_name,
             a.app_source,
             a.customer_intent_city_name,
             a.from_source,
             a.is_distribute,
             a.device_name
         from julive_dim.dim_clue_ext_base a 
         left join (select distinct city_id from julive_dim.dim_wlmq_city) b on a.city_id = b.city_id
         where b.city_id is null
      )t2 on t1.clue_id = t2.clue_id
      where t2.device_name ='APP' and t2.is_distribute != 16
      GROUP BY
          t2.create_date,
          t2.media_name,
          t2.app_type_name,
          t2.app_source,
          t2.customer_intent_city_name
    )
    
 
insert overwrite table julive_app.app_market_app_process_utm_source_nocost 
select 
         coalesce(t.report_date,t1.report_date,t2.report_date,t3.report_date,t4.report_date ,t5.report_date ) as report_date,
         coalesce(t.app_type,t1.app_type,t2.app_type,t3.app_type,t4.app_type ,t5.app_type) as app_type,
         coalesce(t.app_source,t1.app_source,t2.app_source,t3.app_source,t4.app_source ,t5.app_source) as utm_source,
         coalesce(t.city_name,t1.city_name,t2.city_name,t3.city_name,t4.city_name ,t5.city_name) as city_name,
         coalesce(t.media_type,t1.media_type,t2.media_type,t3.media_type,t4.media_type ,t5.media_type) as media_type,
         t.jh_cnt,
         t1.xs_cnt as xs_cnt_time,
         t1.sh_cnt as sh_cnt_time,
         t1.dk_cnt as dk_cnt_time,
         t1.rg_cnt as rg_cnt_time,
         t1.rengou_yingshou as rengou_yingshou_time,
         t2.xs_cnt       as xs_cnt_uv,
         t2.xs_cnt_0d    as xs_cnt_uv_0d,
         t2.xs_cnt_3d    as xs_cnt_uv_3d,
         t2.xs_cnt_7d    as xs_cnt_uv_7d,
         t2.xs_cnt_14d   as xs_cnt_uv_14d,
         t2.xs_cnt_30d   as xs_cnt_uv_30d,
         t2.sh_cnt       as sh_cnt_uv,
         t2.sh_cnt_0d    as sh_cnt_uv_0d,
         t2.sh_cnt_3d    as sh_cnt_uv_3d,
         t2.sh_cnt_7d    as sh_cnt_uv_7d,
         t2.sh_cnt_14d   as sh_cnt_uv_14d,
         t2.dk_cnt       as dk_cnt_uv,
         t2.rg_cnt       as rg_cnt_uv,
         t2.rengou_yingshou as rengou_yingshou_uv,
         t3.sh_cnt_order,
         t3.sh_cnt_order_0d,
         t3.sh_cnt_order_3d,
         t3.sh_cnt_order_7d,
         t3.sh_cnt_order_14d,
         t3.sh_cnt_order_30d,
         t3.dk_cnt_order,
         t3.dk_cnt_order_7d,
         t3.dk_cnt_order_14d,
         t3.dk_cnt_order_30d,
         t3.rg_cnt_order,
         t3.rg_cnt_order_7d,
         t3.rg_cnt_order_14d,
         t3.rg_cnt_order_30d,
        t.jh_cnt_xin,
         t1.xs_cnt_sameday,
         t1.sh_cnt_sameday,
         t4.register_cnt_uv as register_cnt_uv,
         t5.register_cnt_time as register_cnt_time,
         current_timestamp() as etl_time
    from jihuo t
    full join time_process   t1 on 
         t.report_date = t1.report_date and t.city_name = t1.city_name 
         and t.app_type = t1.app_type and t.app_source = t1.app_source
         and t.media_type= t1.media_type
    full join uv_process     t2 on
         coalesce(t.report_date,t1.report_date) = t2.report_date 
         and coalesce(t.city_name,t1.city_name) = t2.city_name 
         and coalesce(t.app_type,t1.app_type) = t2.app_type 
         and coalesce(t.app_source,t1.app_source) = t2.app_source
         and coalesce(t.media_type,t1.media_type)= t2.media_type
    full join order_process  t3 on
         coalesce(t.report_date,t1.report_date,t2.report_date) = t3.report_date 
         and coalesce(t.city_name,t1.city_name,t2.city_name) = t3.city_name 
         and coalesce(t.app_type,t1.app_type,t2.app_type) = t3.app_type 
         and coalesce(t.app_source,t1.app_source,t2.app_source) = t3.app_source
         and coalesce(t.media_type,t1.media_type,t2.media_type) = t3.media_type
    full join register_uv t4 on 
         coalesce(t.report_date,t1.report_date,t2.report_date,t3.report_date ) = t4.report_date 
         and coalesce(t.city_name,t1.city_name,t2.city_name,t3.city_name )  = t4.city_name 
         and coalesce(t.app_type,t1.app_type,t2.app_type,t3.app_type ) = t4.app_type 
         and coalesce(t.app_source,t1.app_source,t2.app_source,t3.app_source) = t4.app_source
         and coalesce(t.media_type,t1.media_type,t2.media_type,t3.media_type) = t4.media_type
    full join register_time t5 on 
         coalesce(t.report_date,t1.report_date,t2.report_date,t3.report_date,t4.report_date ) = t5.report_date 
         and coalesce(t.city_name,t1.city_name,t2.city_name,t3.city_name,t4.city_name )  = t5.city_name 
         and coalesce(t.app_type,t1.app_type,t2.app_type,t3.app_type,t4.app_type ) = t5.app_type 
         and coalesce(t.app_source,t1.app_source,t2.app_source,t3.app_source, t4.app_source) = t5.app_source
         and coalesce(t.media_type,t1.media_type,t2.media_type,t3.media_type,t4.media_type) = t5.media_type
   ;
     
 -- 消耗数据
 with cost as 
    (
         select 
            t.report_date,
            if(t.media_class ='SEM' and t.channel_city_name is not null,t.channel_city_name,t.city_name) as city_name,
            t.app_type,
            if(t.media_class = 'SEM','sem',account.agent) as agent,
            if(report_date >= '2019-11-25' and account.media_type_name = '腾讯智汇推','广点通',account.media_type_name) as media_type,
            sum(t.show_num)  as show_num,
            sum(t.click_num) as click_num,
            round(sum(t.bill_cost),4) as bill_cost,
            round(sum(t.cost),4)      as cost
        from julive_fact.fact_market_area_report_dtl  t
        left join julive_dim.dim_dsp_account_history account on t.account_id = account.id and t.pdate = account.pdate
        where (device_name = 'APP渠道' 
               or (account_id in (538,270,143,45,42,535,7,226) and media_class like 'SEM%'))
        group by t.report_date,
            if(t.media_class ='SEM' and t.channel_city_name is not null,t.channel_city_name,t.city_name) ,
            t.app_type,
            if(t.media_class = 'SEM','sem',account.agent) ,
            if(report_date >= '2019-11-25' and account.media_type_name = '腾讯智汇推','广点通',account.media_type_name)
    )


    --
    insert overwrite table julive_app.app_market_app_process
    -- insert overwrite table tmp_etl.tmp_xl_app_market_app_process
    select 
         coalesce(t.report_date,t4.report_date) as report_date,
         coalesce(t.city_name,t4.city_name) as city_name,
         coalesce(t.app_type,t4.app_type) as app_type,
         coalesce(t.agent,t4.agent) as agent,
         coalesce(t.media_type,t4.media_type) as media_type,
         t4.cost,
         t.jh_cnt,
         t.xs_cnt_time,
         t.sh_cnt_time,
         t.dk_cnt_time,
         t.rg_cnt_time,
         t.rengou_yingshou_time,
         t.xs_cnt_uv,
         t.xs_cnt_uv_0d,
         t.xs_cnt_uv_3d,
         t.xs_cnt_uv_7d,
         t.xs_cnt_uv_14d,
         t.xs_cnt_uv_30d,
         t.sh_cnt_uv,
         t.sh_cnt_uv_0d,
         t.sh_cnt_uv_3d,
         t.sh_cnt_uv_7d,
         t.sh_cnt_uv_14d,
         t.dk_cnt_uv,
         t.rg_cnt_uv,
         t.rengou_yingshou_uv,
         t.sh_cnt_order,
         t.sh_cnt_order_0d,
         t.sh_cnt_order_3d,
         t.sh_cnt_order_7d,
         t.sh_cnt_order_14d,
         t.sh_cnt_order_30d,
         t.dk_cnt_order,
         t.dk_cnt_order_7d,
         t.dk_cnt_order_14d,
         t.dk_cnt_order_30d,
         t.rg_cnt_order,
         t.rg_cnt_order_7d,
         t.rg_cnt_order_14d,
         t.rg_cnt_order_30d,
         t4.show_num,
         t4.click_num,
         t.jh_cnt_xin,
         t.xs_cnt_sameday,
         t.sh_cnt_sameday,
         t4.bill_cost,
         t.register_cnt_uv,
         t.register_cnt_time,
         current_timestamp() as etl_time
    from 
    (SELECT
                media_type,
                app_type,
                if(split(utm_source,'_')[0] = 'feedbaidu','wkui',
                if(instr(utm_source,'jl_lp_2') = 1,'wy',split(utm_source,'_')[0])) as agent,
                city_name,
                report_date,
                sum(jh_cnt) as jh_cnt,
              sum(jh_cnt_xin) as jh_cnt_xin,
                sum(xs_cnt_time) as xs_cnt_time,
                sum(sh_cnt_time) as sh_cnt_time,
                sum(dk_cnt_time) as dk_cnt_time,
                sum(rg_cnt_time) as rg_cnt_time,
                sum(rengou_yingshou_time) as rengou_yingshou_time,
                sum(xs_cnt_uv) as xs_cnt_uv,
                sum(xs_cnt_uv_0d) as xs_cnt_uv_0d,
                sum(xs_cnt_uv_3d) as xs_cnt_uv_3d,
                sum(xs_cnt_uv_7d) as xs_cnt_uv_7d,
                sum(xs_cnt_uv_14d) as xs_cnt_uv_14d,
                sum(xs_cnt_uv_30d) as xs_cnt_uv_30d,
                sum(sh_cnt_uv) as sh_cnt_uv,
                sum(sh_cnt_uv_0d) as sh_cnt_uv_0d,
                sum(sh_cnt_uv_3d) as sh_cnt_uv_3d,
                sum(sh_cnt_uv_7d) as sh_cnt_uv_7d,
                sum(sh_cnt_uv_14d) as sh_cnt_uv_14d,
                sum(dk_cnt_uv) as dk_cnt_uv,
                sum(rg_cnt_uv) as rg_cnt_uv,
                sum(rengou_yingshou_uv) as rengou_yingshou_uv,
                sum(sh_cnt_order) as sh_cnt_order,
                sum(sh_cnt_order_0d) as sh_cnt_order_0d,
                sum(sh_cnt_order_3d) as sh_cnt_order_3d,
                sum(sh_cnt_order_7d) as sh_cnt_order_7d,
                sum(sh_cnt_order_14d) as sh_cnt_order_14d,
                sum(sh_cnt_order_30d) as sh_cnt_order_30d,
                sum(dk_cnt_order) as dk_cnt_order,
                sum(dk_cnt_order_7d) as dk_cnt_order_7d,
                sum(dk_cnt_order_14d) as dk_cnt_order_14d,
                sum(dk_cnt_order_30d) as dk_cnt_order_30d,
                sum(rg_cnt_order) as rg_cnt_order,
                sum(rg_cnt_order_7d) as rg_cnt_order_7d,
                sum(rg_cnt_order_14d) as rg_cnt_order_14d,
                sum(rg_cnt_order_30d) as rg_cnt_order_30d,
                sum(xs_cnt_sameday) as xs_cnt_sameday,
                sum(sh_cnt_sameday) as sh_cnt_sameday,
                sum(register_cnt_uv) as register_cnt_uv,
                sum(register_cnt_time) as register_cnt_time
        FROM julive_app.app_market_app_process_utm_source_nocost
        GROUP BY
                media_type,
                app_type,
                if(split(utm_source,'_')[0] = 'feedbaidu','wkui',
                if(instr(utm_source,'jl_lp_2') = 1,'wy',split(utm_source,'_')[0])),
                city_name,
                report_date
        )  t
    full join cost           t4 on
         t.report_date = t4.report_date 
         and t.city_name  = t4.city_name 
         and t.app_type = t4.app_type 
         and t.agent = t4.agent
         and t.media_type = t4.media_type
    
   ;
   
INSERT OVERWRITE TABLE tmp_bi.app_process_agent
select 
         t.report_date,
         t.city_name,
         t.app_type,
         t.agent,
         t.media_type,
         t.cost,
         t.jh_cnt,
         t.xs_cnt_time,
         t.sh_cnt_time,
         t.dk_cnt_time,
         t.rg_cnt_time,
         t.rengou_yingshou_time,
         t.xs_cnt_uv,
         t.xs_cnt_uv_0d,
         t.xs_cnt_uv_3d,
         t.xs_cnt_uv_7d,
         t.xs_cnt_uv_14d,
         t.xs_cnt_uv_30d,
         t.sh_cnt_uv,
         t.sh_cnt_uv_0d,
         t.sh_cnt_uv_3d,
         t.sh_cnt_uv_7d,
         t.sh_cnt_uv_14d,
         t.dk_cnt_uv,
         t.rg_cnt_uv,
         t.rengou_yingshou_uv,
         t.sh_cnt_order,
         t.sh_cnt_order_0d,
         t.sh_cnt_order_3d,
         t.sh_cnt_order_7d,
         t.sh_cnt_order_14d,
         t.sh_cnt_order_30d,
         t.dk_cnt_order,
         t.dk_cnt_order_7d,
         t.dk_cnt_order_14d,
         t.dk_cnt_order_30d,
         t.rg_cnt_order,
         t.rg_cnt_order_7d,
         t.rg_cnt_order_14d,
         t.rg_cnt_order_30d,
         t.`show`,
         t.`click`,
         t.jh_cnt_xin,
         t.xs_cnt_sameday,
         t.sh_cnt_sameday,
         t.bill_cost,
         t.register_cnt_uv,
         t.register_cnt_time
  from julive_app.app_market_app_process t;
  
  
  
drop view if exists tmp_bi.app_process_utm_source_nocost;
create view if not exists tmp_bi.app_process_utm_source_nocost as 
select 
    media_type	,	
    app_type	,	
    utm_source	,	
    city_name	,	
    report_date	,	
    xs_cnt_time	,	
    sh_cnt_time	,	
    dk_cnt_time	,	
    rg_cnt_time	,	
    rengou_yingshou_time,
    xs_cnt_uv	,	
    xs_cnt_uv_0d	,	
    xs_cnt_uv_3d	,	
    xs_cnt_uv_7d	,	
    xs_cnt_uv_14d	,	
    xs_cnt_uv_30d	,	
    sh_cnt_uv	,	
    sh_cnt_uv_0d	,	
    sh_cnt_uv_3d	,	
    sh_cnt_uv_7d	,	
    sh_cnt_uv_14d	,	
    dk_cnt_uv	,	
    rg_cnt_uv	,	
    rengou_yingshou_uv,
    sh_cnt_order	,	
    sh_cnt_order_0d	,	
    sh_cnt_order_3d	,	
    sh_cnt_order_7d	,	
    sh_cnt_order_14d	,	
    sh_cnt_order_30d	,	
    dk_cnt_order	,	
    dk_cnt_order_7d	,	
    dk_cnt_order_14d	,	
    dk_cnt_order_30d	,	
    rg_cnt_order	,	
    rg_cnt_order_7d	,	
    rg_cnt_order_14d	,	
    rg_cnt_order_30d	,	
    jh_cnt	,	
    jh_cnt_xin	,	
    xs_cnt_sameday	,	
    sh_cnt_sameday,
    register_cnt_uv,
    register_cnt_time
from julive_app.app_market_app_process_utm_source_nocost
;
