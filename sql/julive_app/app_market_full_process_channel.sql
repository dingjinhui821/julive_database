set spark.app.name=app_market_full_process_channel;
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.executor.cores=1;                    --使用的cpu核数, 多数情况1就满足
set spark.executor.memory=2g;                  --使用的内存,通常1-2g足够
set spark.executor.instances=12;

set etl_date = '${hiveconf:etlDate}';
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

-- set etl_yestoday = '${hiveconf:etlYestoday}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：APP-市场全流程渠道城市数据表
--   输 入 表 ：
--         julive_fact.fact_market_area_report_dtl                -- 日-城市-市场展点销 明细表
--         julive_app.app_market_offline_conversion_market        -- APP-市场线下数据表

--   输 出 表：julive_app.app_market_full_process_channel
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/23 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容




with 
-- 线上数据
cost as (
    select  
        t.report_date,
        if(t.media_class ='SEM' and t.device_name in ('PC','M'),
            if(instr(t.channel_name,'DSA')>0 or instr(t.channel_name,'dsa')>0,
                t.channel_city_name,
                coalesce(t.url_city_name,t.channel_city_name,t.kw_city_name)
            ),
        city_name) as city_name,
        if(t.report_date >= '2019-11-25' and account.media_type_name = '腾讯智汇推','广点通',account.media_type_name) as media_type,
        if(t.device_name = 'APP渠道',t.device_name,if(t.device_name like '%小程序%','小程序', t.media_class)) as product_type,
        if(t.device_name = 'APP渠道', t.app_type,t.device_name) as device_type,
        0 as yw_line,
        t.channel_id,
        t.channel_name,
        t.app_type,
        sum(t.show_num)  as show_num,
        sum(t.click_num) as click_num,
        round(sum(t.bill_cost),4) as bill_cost,
        round(sum(t.cost),4)      as cost
    from julive_fact.fact_market_area_report_dtl  t
    join julive_dim.dim_dsp_account_history account on t.account_id = account.id and t.pdate = account.pdate
    
    group by 
        t.report_date,
        if(t.media_class ='SEM' and t.device_name in ('PC','M'),
            if(instr(t.channel_name,'DSA')>0 or instr(t.channel_name,'dsa')>0,
                t.channel_city_name,
                coalesce(t.url_city_name,t.channel_city_name,t.kw_city_name)
            ),
        city_name),
        if(t.report_date >= '2019-11-25' and account.media_type_name = '腾讯智汇推','广点通',account.media_type_name),
        if(t.device_name = 'APP渠道',t.device_name,if(t.device_name like '%小程序%','小程序',t. media_class)) ,
        if(t.device_name = 'APP渠道', t.app_type,t.device_name),
        t.channel_id,
        t.channel_name,
        t.app_type
),
   

-- 线下数据
offline as (
    select 
        t.report_date,
        t1.city_name,
        if(t1.module_name ='APP渠道',t1.app_type_name,t1.device_name) as device_type,
        t.media_type as media_type,
        t1.module_name as product_type,
        t.yw_line,
        t1.app_type_name as app_type,
        t1.channel_id,
        t1.channel_name,
        sum(t.xs_cnt) as xs_cnt,
        sum(t.sh_cnt) as sh_cnt,
        sum(t.dk_cnt) as dk_cnt,
        sum(t.rg_cnt) as rg_cnt,
        sum(t.qy_cnt) as qy_cnt,
        sum(t.rengou_yingshou) as rengou_yingshou,
        sum(t.rengou_yingshou_net) as rengou_yingshou_net,
        sum(t.qianyue_yingshou) as qianyue_yingshou,
        sum(t.probs) as probs,
        sum(t.`400_xs_cnt`) as `400_xs_cnt`,
        sum(t.developer_xs_cnt) as developer_xs_cnt,
        sum(t.jietong_sh_day) as jietong_sh_day,
        sum(t.xs_score) as xs_score,
        sum(t.first_call_duration) as first_call_duration,
        sum(t.intent_low_num) as intent_low_num,
        sum(t.xs_cnt_all) as xs_cnt_all,
        sum(t.first_call_duration_num) as first_call_duration_num,
    	  sum(t.online_dk_cnt) as online_dk_cnt,
    	  sum(t.rg_cnt_net) as rg_cnt_net,
    	  sum(t.`400_sh_cnt`) as `400_sh_cnt`
    from julive_app.app_market_offline_conversion_market t 
    left join julive_dim.dim_channel_info t1 on t.channel_id = t1.channel_id
    group by 
        t.report_date,
        t1.city_name,
        if(t1.module_name ='APP渠道',t1.app_type_name,t1.device_name),
        t.media_type  ,
        t1.module_name,
        t.yw_line,
        t1.app_type_name,
        t1.channel_id,
        t1.channel_name
        
)


insert overwrite table julive_app.app_market_full_process_channel
select 
    coalesce(t1.report_date,t2.report_date) as report_date,
    coalesce(t1.city_name,t2.city_name) as city,
    coalesce(t1.device_type,t2.device_type) as device_type,
    coalesce(t1.media_type,t2.media_type) as media_type,
    coalesce(t1.product_type,t2.product_type) as product_type,
    coalesce(t1.app_type,t2.app_type) as app_type,
    coalesce(t1.channel_id,t2.channel_id) as channel_id,
    coalesce(t1.channel_name,t2.channel_name) as channel_name,
    t2.show_num,
    t2.click_num,
    t2.bill_cost,
    t2.cost,
    t1.xs_cnt,
    t1.sh_cnt,
    t1.dk_cnt,
    t1.rg_cnt,
    t1.qy_cnt,
    t1.rengou_yingshou,
    t1.rengou_yingshou_net,
    t1.qianyue_yingshou,
    t1.probs,
    null as city_group,
    dim_city.mgr_city as zhufucity_dabao,
    dim_city.region,
    dim_city.city_type,
    t1.400_xs_cnt,
    t1.developer_xs_cnt,
    t1.jietong_sh_day,
    t1.xs_score,
    t1.first_call_duration,
    t1.intent_low_num,
    t1.xs_cnt_all ,
	  t1.first_call_duration_num,
	  t1.online_dk_cnt,
    t1.rg_cnt_net,
   	t1.400_sh_cnt,
    coalesce(t1.yw_line,t2.yw_line) as yw_line
from offline t1
full join cost t2 on t1.report_date = t2.report_date 
    and t1.city_name = t2.city_name
    and t1.media_type = t2.media_type
    and t1.product_type = t2.product_type
    and t1.device_type = t2.device_type
    and t1.yw_line = t2.yw_line
    and t1.app_type = t2.app_type
    and t1.channel_id = t2.channel_id
    and t1.channel_name = t2.channel_name
left join julive_dim.dim_city on coalesce(t1.city_name,t2.city_name) = dim_city.city_name   
where coalesce(t1.report_date,t2.report_date) >= '2019-01-01'
;
 
 
 insert overwrite table tmp_bi.market_full_process_channel
 select
     report_date ,
     city,
     device_type ,
     media_type ,
     product_type ,
     sum(show_num) as show_num,
     sum(click_num) as  click_num,
     sum(bill_cost) as  bill_cost,
     sum(cost) as  cost,
     sum(xs_cnt) as  xs_cnt,
     sum(sh_cnt) as  sh_cnt,
     sum(dk_cnt) as  dk_cnt,
     sum(rg_cnt) as  rg_cnt,
     sum(qy_cnt) as  qy_cnt,
     sum(rengou_yingshou)     as  rengou_yingshou,
     sum(rengou_yingshou_net) as  rengou_yingshou_net,
     sum(qianyue_yingshou)    as  qianyue_yingshou,
     sum(probs)           as  probs,
     null      as  city_group,
     zhufucity_dabao ,
     region,
     city_type,
     sum(400_xs_cnt)       as  400_xs_cnt,
     sum(developer_xs_cnt) as  developer_xs_cnt,
     sum(jietong_sh_day)   as  jietong_sh_day,
     sum(xs_score)         as  xs_score,
     sum(first_call_duration) as  first_call_duration,
     sum(intent_low_num) as intent_low_num ,
     sum(xs_cnt_all)     as  xs_cnt_all,
     sum(first_call_duration_num) as  first_call_duration_num,
     sum(online_dk_cnt)           as  online_dk_cnt,
     sum(rg_cnt_net) as  rg_cnt_net,
     sum(400_sh_cnt) as  400_sh_cnt,
     yw_line
 from julive_app.app_market_full_process_channel
 group by 
     report_date ,
     city,
     device_type ,
     media_type ,
     product_type,
     yw_line,
     region,
     city_type,
     zhufucity_dabao
 ;

 insert overwrite table tmp_bi.market_full_process_channel_backup_day
 partition (pdate)
 select
     report_date ,
     city,
     device_type ,
     media_type ,
     product_type ,
     show_num ,
     click_num ,
     bill_cost ,
     cost ,
     xs_cnt ,
     sh_cnt ,
     dk_cnt ,
     rg_cnt ,
     qy_cnt ,
     rengou_yingshou ,
     rengou_yingshou_net ,
     qianyue_yingshou ,
     probs ,
     city_group ,
     zhufucity_dabao ,
     region ,
     city_type ,
     400_xs_cnt ,
     developer_xs_cnt ,
     jietong_sh_day ,
     xs_score ,
     first_call_duration ,
     intent_low_num ,
     xs_cnt_all ,
     first_call_duration_num ,
     online_dk_cnt ,
     rg_cnt_net ,
     400_sh_cnt ,
     ${hiveconf:etl_date} as pdate
 from tmp_bi.market_full_process_channel;
 
 
insert overwrite table tmp_bi.market_app_process_channel
select
    report_date,	
    app_type,	
    city,	
    'APP' as device_type,	
    media_type,	
    product_type,	
    sum(show_num) as    show_num,	
    sum(click_num) as 	click_num	,	
    sum(bill_cost) as 	bill_cost	,	
    sum(cost)      as 	cost	,	
    sum(xs_cnt)    as 	xs_cnt	,	
    sum(sh_cnt)    as 	sh_cnt	,	
    sum(dk_cnt)    as	  dk_cnt	,	
    sum(rg_cnt)    as	  rg_cnt	,	
    sum(qy_cnt)    as	  qy_cnt	,	
    sum(rengou_yingshou)        as	  rengou_yingshou	,	
    sum(rengou_yingshou_net)    as	  rengou_yingshou_net	,
    sum(qianyue_yingshou)       as	  qianyue_yingshou	,	
    sum(probs)                  as		probs	,	
    sum(`400_xs_cnt`)           as		`400_xs_cnt`	,	
    sum(developer_xs_cnt)       as		developer_xs_cnt,		
    sum(jietong_sh_day)         as		jietong_sh_day	,	
    sum(xs_score)               as		xs_score	,	
    sum(first_call_duration)    as		first_call_duration	,	
    sum(intent_low_num)         as		intent_low_num	,	
    sum(xs_cnt_all)             as		xs_cnt_all	,	
    sum(first_call_duration_num)   as		first_call_duration_num	,	
    sum(online_dk_cnt)             as	  online_dk_cnt	,	
    sum(rg_cnt_net)                as		rg_cnt_net	,	
    sum(`400_sh_cnt`)              as		`400_sh_cnt`,
    yw_line	
from julive_app.app_market_full_process_channel
where product_type = 'APP渠道'
group by 
    report_date,	
    app_type,	
    city,	
    media_type,	
    product_type,
    yw_line
;

drop view dwd.market_app_process;
create view dwd.market_app_process as
select * from tmp_bi.market_app_process_channel;  


INSERT OVERWRITE TABLE tmp_bi.market_feed_process_channel
select
    report_date,	
    city,	
    device_type,	
    media_type,	
    product_type,	
    sum(show_num) as    show_num,	
    sum(click_num) as 	click_num	,	
    sum(bill_cost) as 	bill_cost	,	
    sum(cost)      as 	cost	,	
    sum(xs_cnt)    as 	xs_cnt	,	
    sum(sh_cnt)    as 	sh_cnt	,	
    sum(dk_cnt)    as	  dk_cnt	,	
    sum(rg_cnt)    as	  rg_cnt	,	
    sum(qy_cnt)    as	  qy_cnt	,	
    sum(rengou_yingshou)        as	  rengou_yingshou	,	
    sum(rengou_yingshou_net)    as	  rengou_yingshou_net	,
    sum(qianyue_yingshou)       as	  qianyue_yingshou	,	
    sum(probs)                  as		probs	,	
    sum(`400_xs_cnt`)           as		`400_xs_cnt`	,	
    sum(developer_xs_cnt)       as		developer_xs_cnt,		
    sum(jietong_sh_day)         as		jietong_sh_day	,	
    sum(xs_score)               as		xs_score	,	
    sum(first_call_duration)    as		first_call_duration	,	
    sum(intent_low_num)         as		intent_low_num	,	
    sum(xs_cnt_all)             as		xs_cnt_all	,	
    sum(first_call_duration_num)   as		first_call_duration_num	,	
    sum(online_dk_cnt)             as	  online_dk_cnt	,	
    sum(rg_cnt_net)                as		rg_cnt_net	,	
    sum(`400_sh_cnt`)              as		`400_sh_cnt`,
    yw_line
from julive_app.app_market_full_process_channel
where product_type = 'feed'
group by 
    report_date,	
    city,
    device_type,		
    media_type,	
    product_type,
    yw_line
;

drop view dwd.market_feed_process;
create view dwd.market_feed_process as
select * from tmp_bi.market_feed_process_channel;



INSERT OVERWRITE TABLE tmp_bi.market_sem_process_channel
select
    report_date,
    channel_id,
    channel_name,
    city,	
    device_type,	
    media_type,	
    product_type,	
    sum(show_num) as    show_num,	
    sum(click_num) as 	click_num	,	
    sum(bill_cost) as 	bill_cost	,	
    sum(cost)      as 	cost	,	
    sum(xs_cnt)    as 	xs_cnt	,	
    sum(sh_cnt)    as 	sh_cnt	,	
    sum(dk_cnt)    as	  dk_cnt	,	
    sum(rg_cnt)    as	  rg_cnt	,	
    sum(qy_cnt)    as	  qy_cnt	,	
    sum(rengou_yingshou)        as	  rengou_yingshou	,	
    sum(rengou_yingshou_net)    as	  rengou_yingshou_net	,
    sum(qianyue_yingshou)       as	  qianyue_yingshou	,	
    0 as is_esf_sell,
    0 as is_esf,
    sum(probs)                  as		probs	,	
    city_group,
    zhufucity_dabao,
    sum(`400_xs_cnt`)           as		`400_xs_cnt`	,	
    sum(developer_xs_cnt)       as		developer_xs_cnt,		
    sum(jietong_sh_day)         as		jietong_sh_day	,	
    sum(xs_score)               as		xs_score	,	
    sum(first_call_duration)    as		first_call_duration	,	
    sum(intent_low_num)         as		intent_low_num	,	
    sum(xs_cnt_all)             as		xs_cnt_all	,	
    sum(first_call_duration_num)   as		first_call_duration_num	,	
    sum(online_dk_cnt)             as	  online_dk_cnt	,	
    sum(rg_cnt_net)                as		rg_cnt_net	,	
    sum(`400_sh_cnt`)              as		`400_sh_cnt`,
    yw_line
from julive_app.app_market_full_process_channel
where product_type = 'SEM'
group by 
    report_date,
    channel_id,
    channel_name,
    city,
    device_type,		
    media_type,	
    product_type,
    yw_line,
    city_group,
    zhufucity_dabao
;

drop view dwd.market_sem_process;
create view dwd.market_sem_process as
select * from tmp_bi.market_sem_process_channel;
