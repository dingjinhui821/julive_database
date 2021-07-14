set hive.execution.engine=spark;
set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=4g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=12;
set spark.yarn.queue=etl;
set spark.files.fetchTimeout=300s;
set spark.app.name=fact_event_sign_up_base_dtl;



set etl_date = '${hiveconf:etlDate}';
set etl_yestoday = '${hiveconf:etlYestoday}'; 

--   功能描述：FACT-注册事件日志

--   输 入 表 ：
--         julive_fact.fact_event_dtl                       -- 埋点数据明细事实基表
--         dwd.dwd_appinstall_channel_match_by_global       -- AppInstall时的channel表
--         ods.dsp_creative_report                          -- 媒体创意报告
--         

--   输 出 表：julive_fact.fact_event_sign_up_base_dtl
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/23 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

    

    drop table if exists tmp_etl.tmp_fact_event_sign_up_base_dtl_event_sign_up;
    create table if not exists tmp_etl.tmp_fact_event_sign_up_base_dtl_event_sign_up as 
    select 
        global_id,
        create_datetime,
        julive_id,
        channel_id,
        channel_put,
        app_id,
        product_id,
        select_city,
        `$utm_source`,
        event,
        os,
        os_version,
        channel,
        `$city`
    from (
        select 
            t.global_id,
            cast(t.create_timestamp/1000 as int) as create_datetime,
            if(event='app__v1__real__register2',a.register_user_id,t.julive_id) as julive_id,
            t.channel_id,
            t.channel_put,
            t.app_id,
            t.product_id,
            t.select_city,
            if (instr(a.`utm_source`,'gdt')<>0, split(a.adgroup_name, '\\|')[0], nvl(a.`utm_source`, a.channel)) as `$utm_source`,
            t.event,
            a.os,
            a.os_version,
            case when cast(product_id as decimal)=201 AND a.channel IS NULL then 'appstore'
                 when cast(product_id as decimal)=201002 AND a.channel IS NULL then 'mjappstore'
                 when cast(product_id as decimal)=201004 AND a.channel IS NULL then 'jisuappstore'
                 else a.channel
            end as channel,
            ip_trs(`$ip`,'city') as `$city`,
            row_number() over (partition by t.global_id order by t.create_timestamp desc) as rn
        from julive_fact.fact_event_dtl t 
        lateral view json_tuple(properties,
        'channel','utm_source','$utm_term', '$utm_campaign', '$utm_matching_type','$utm_medium','$utm_content',
        'akey','app_id', '$carrier', 'manufacturer', 'model', '$ip', 'app_version',
        'comjia_android_id', 'comjia_device_id', 'comjia_imei', 'comjia_unique_id', 'os', 'os_version',
        '$is_first_day', 'select_city', 'FirstUseTime', '$user_agent', 'idfa', 'device_type', 'IDFV', 'adgroup_name','register_user_id') a
        as channel,`utm_source`,`$utm_term`, `$utm_campaign`, `$utm_matching_type`,`$utm_medium`,`$utm_content`,
        akey,app_id, `$carrier`, manufacturer, model, `$ip`, app_version,
        comjia_android_id, comjia_device_id, comjia_imei, comjia_unique_id, os, os_version,
        `$is_first_day`, select_city, FirstUseTime, `$user_agent`, idfa, device_type, IDFV, adgroup_name,register_user_id
         where event in ('e_click_sign_up','app__v1__real__register2') and pdate = ${hiveconf:etl_date}
    ) tt
    where tt.rn = 1
    ;

    drop table if exists tmp_etl.tmp_fact_event_sign_up_base_dtl_appinstall_list;
    create table if not exists tmp_etl.tmp_fact_event_sign_up_base_dtl_appinstall_list as 
    with unit_id_name AS
    (
        SELECT unit_name,
               max(unit_id) as unit_id
        FROM ods.dsp_creative_report
        WHERE --dsp_account_id = 551
           unit_name IS NOT NULL AND from_unixtime(report_date,'yyyy-MM')>= '2020-12'
        group by unit_name
    )  
    select t.global_id,
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
       select global_id from tmp_etl.tmp_fact_event_sign_up_base_dtl_event_sign_up
    ) t2 on t.global_id = t2.global_id;
    
    

 
with 
sign_up as (
    select * from tmp_etl.tmp_fact_event_sign_up_base_dtl_event_sign_up
),

appinstall_list as 
(    
    select * from tmp_etl.tmp_fact_event_sign_up_base_dtl_appinstall_list
),
appinstall_range as 
(  
    select 
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

insert overwrite table  julive_fact.fact_event_sign_up_base_dtl partition(pdate=${hiveconf:etl_date})
select 
    t1.global_id,
    t1.create_datetime,
    t1.julive_id,
    t1.channel_id,
    t1.channel_put,
    t1.app_id,
    t1.product_id,
    t1.select_city,
    coalesce(t2.utm_source,t1.`$utm_source`) as `$utm_source`,
    t1.event,
    t1.os,
    t1.os_version,
    coalesce(t2.channel,t1.channel) as channel,
    t1.`$city`,
    t2.install_date_time as install_datetime,
    t2.aid,
    t2.cid,
    current_timestamp() as etl_time
from sign_up t1
left join appinstall_range t2 on t1.global_id = t2.global_id
where
    (( to_date(from_unixtime(t1.create_datetime,'yyyy-MM-dd')) >= to_date(t2.install_date_time) 
            and to_date(from_unixtime(t1.create_datetime,'yyyy-MM-dd')) < to_date(t2.after_install_date_time)
    )) or t2.global_id is null
;
