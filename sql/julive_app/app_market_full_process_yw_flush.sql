--  功能描述：市场各环节数据（业务城市）,每天全量重跑，分为8:00/10:00/12:00/14:00 各跑一次，依赖线上消耗表和线下已经计算好的临时表                          
--  输 入 表：
--        julive_dim.dim_clue_base_info                                 -- 线索维度基表-包含多条业务线数据 from_source区分
--        julive_dim.dim_dsp_account_history                            -- DSP账户历史表纬度表
--        julive_dim.dim_city                                           -- 城市纬度表
--        julive_dim.dim_channel_info                                   -- 渠道维度表

--        dwd.consultant_called_log_clue_report                         -- 通话相关        
--        ods.yw_sys_number_talking                                     -- 通话相关
--        julive_fact.fact_see_project_dtl                              -- 带看事实表(自营)
--        julive_fact.fact_subscribe_dtl                                -- 认购事实表(自营)
--        julive_fact.fact_sign_base_dtl                                -- 签约事实表
--        julive_fact.fact_market_order_rel_appinstall                  -- 订单关联AppInstall表
--        julive_fact.fact_kfsclue_full_line_indi                       -- 开发商线索维度基表
--        julive_fact.fact_market_area_report_dtl                       -- 日-城市-市场展点销 明细表
--
--  输 出 表：julive_app.app_market_full_process_yw                      -- 市场投放各环节数据(数仓业务城市版)表
--           tmp_bi.market_full_process_yw                              -- 李宁-市场投放各环节数据(数仓业务城市版)表
--
--  创 建 者：  姜宝桥  18600505375  jiangbaoqiao@julive.com
--  创建日期： 2021/06/11 15:16
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--  
-- set hive.execution.engine=spark;
set spark.app.name=app_market_full_process_yw;
set mapred.job.name=app_market_full_process_yw;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=3;
set spark.executor.memory=8g;
set spark.executor.instances=14;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

-- 一.创建临时表，通话次数、通话时长处理（忽略-已处理）tmp_etl.tmp_market_full_process_yw_01
-- 二.明细层，线下各个流程节点明细数据（忽略-已处理）tmp_etl.tmp_market_full_process_yw_02
-- 三.修复媒体名称、设备类型（忽略-已处理）tmp_etl.tmp_market_full_process_yw_03
-- 四.业务城市渠道处理（忽略-已处理）tmp_etl.tmp_market_full_process_yw_04

-- 五.sem 业务城市渠道处理过程
-- 按照上户城市拆分
drop table if exists tmp_etl.tmp_market_full_process_yw_05;
create table if not exists tmp_etl.tmp_market_full_process_yw_05 as 
select 
          sem_online.report_date,
          sem_online.channel_id,
          sem_online.device_type,
          sem_online.media_type,
          sem_online.product_type,  
          coalesce(sem_offline.city_name,sem_online.channel_city_name) as city_name,
          if(sem_offline.total_sh_cnt > 0 ,sem_offline.sh_cnt/sem_offline.total_sh_cnt, 1) as sh_per,
          sem_online.show_num * if(sem_offline.total_sh_cnt > 0 ,sem_offline.sh_cnt/sem_offline.total_sh_cnt, 1) as show_num,
          sem_online.click_num * if(sem_offline.total_sh_cnt > 0 ,sem_offline.sh_cnt/sem_offline.total_sh_cnt, 1) as click_num,
          sem_online.bill_cost * if(sem_offline.total_sh_cnt > 0 ,sem_offline.sh_cnt/sem_offline.total_sh_cnt, 1) as bill_cost,
          sem_online.cost * if(sem_offline.total_sh_cnt > 0 ,sem_offline.sh_cnt/sem_offline.total_sh_cnt, 1) as cost,
          current_timestamp() as etl_time -- 插入时间 
      from (   
          select 
              t.report_date,
              t.channel_id,
              t.channel_name,
              t.channel_city_name,
              t.device_name as device_type,
              t1.media_type_name as media_type,
              'SEM' product_type,
              sum(t.show_num) as show_num,
              sum(t.click_num) as click_num,
              sum(t.bill_cost) as bill_cost,
              sum(t.cost) as cost
          from julive_fact.fact_market_area_report_dtl  t  
          join julive_dim.dim_dsp_account_history t1 on t.account_id = t1.id and t.pdate = t1.pdate
          where t.source = 'SEM' and t.device_name <> 'APP渠道'
          group by t.report_date,
              t.channel_id,
              t.channel_name,
              t.device_name,
              t.channel_city_name,
              t1.media_type_name
      ) sem_online        
      left join (    
          select
              report_date,
              city_name,  
              device_type,
              channel_id,
              sh_cnt,
              sum(sh_cnt) over (partition by report_date, device_type, channel_id) as total_sh_cnt
          from (
              SELECT  t.report_date,
                      t.city_name,
                      t.device_type,
                      t.channel_id,
                      sum(sh_cnt) as sh_cnt
              from  tmp_etl.tmp_market_full_process_yw_04 t
              WHERE product_type = 'SEM'  
               and t.sh_cnt>0
              group by t.report_date,
                      t.city_name,  
                      t.device_type,
                      t.channel_id
          ) tab 
      )sem_offline 
  on sem_online.report_date = sem_offline.report_date
  and sem_online.channel_id = sem_offline.channel_id
  and sem_online.device_type = sem_offline.device_type
  ;


-- 六.关联展示、点击、消耗
insert overwrite table julive_app.app_market_full_process_yw
select
    xx.report_date, -- 报告日期
    xx.city_name,-- 业务城市名称
    yy.region,-- 所属大区
    yy.city_type_first as city_type, -- 新老城（主城含副城）
    yy.mgr_city,-- 主城名称
    case when xx.city_name in('北京' ,'上海' ,'天津' ,'广州' ,'深圳' ,'杭州' ,'苏州' ,'成都','重庆' ,'郑州' ,'南京' ,'佛山') then '老城' else '新城' end as city_new_old, -- 新老城（主城不含副城）
    xx.media_type, -- 媒体名称：是修复后的数据
    xx.product_type, -- 模块名称
    xx.device_type, --设备名称：是修复后的数据
    xx.from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    coalesce(xx.if_refer,0) as if_refer, -- 是否友介 1:是，0否
		sum(xx.show_num) as show_num, -- 展示
		sum(xx.click_num) as click_num,-- 点击
		sum(xx.bill_cost) as bill_cost,-- 帐面消耗
		sum(xx.cost) as cost,-- 消耗
    sum(xx.xs_cnt) as xs_cnt, --线索量
    sum(xx.sh_cnt) as sh_cnt, --上户量
    sum(xx.dk_cnt) as dk_cnt, --带看量
    sum(xx.rg_cnt) as rg_cnt, --认购量
    sum(xx.qy_cnt) as qy_cnt, --签约量
    sum(xx.rengou_yingshou) as rengou_yingshou, --认购应收含退外联
    sum(xx.rengou_yingshou_net) as rengou_yingshou_net,--认购应收净
    sum(xx.qianyue_yingshou) as qianyue_yingshou,--签约应收含退外联
    sum(xx.xs_score) as xs_score, -- 线索质量分 需要扩展
    sum(xx.first_call_duration) as first_call_duration, -- 首次通话时长 需要扩展
    sum(xx.first_call_duration_num) as first_call_duration_num, -- 首次通话时数量 需要扩展
    sum(xx.online_dk_cnt) as online_dk_cnt, -- 线上带看量
    sum(xx.400_xs_cnt)   as 400_xs_cnt,   -- 400线索量  
    sum(xx.400_sh_cnt) as 400_sh_cnt,     -- 400上户量 
    sum(xx.intent_low_num) as intent_low_num, -- 当日上户关闭订单
    sum(xx.call_duration_sh_num) as call_duration_sh_num, -- 通话上户数量
    sum(coalesce(xx.developer_xs_cnt,0)) as developer_xs_cnt, -- 开发商线索数量
    sum(coalesce(xx.developer_xs_cnt,0)+coalesce(xx.xs_cnt,0)) as xs_all_cnt, -- 线索总数量
    current_timestamp() as etl_time       -- 插入时间 
from
(
--- 1.自营部分
select 
   t.report_date             as report_date,
   t.city_name               as city_name,-- 业务城市
   t.media_type              as media_type,  -- 媒体名称：是修复后的数据
   t.product_type            as product_type, -- 模块名称
   t.device_type             as device_type,-- 设备名称：是修复后的数据
   t.from_source             as from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
   t.if_refer                as if_refer, -- 是否友介 1:是，0否
   null                      as show_num,-- 展示
   null 										 as click_num,-- 点击
   null                      as bill_cost,-- 帐面消耗
   null                      as cost,--消耗
    sum(t.xs_cnt) as xs_cnt, --线索量
    sum(t.sh_cnt) as sh_cnt, --上户量
    sum(t.dk_cnt) as dk_cnt, --带看量
    sum(t.rg_cnt) as rg_cnt, --认购量
    sum(t.qy_cnt) as qy_cnt, --签约量
    sum(t.rengou_yingshou) as rengou_yingshou, --认购应收含退外联
    sum(t.rengou_yingshou_net) as rengou_yingshou_net,--认购应收净
    sum(t.qianyue_yingshou) as qianyue_yingshou,--签约应收含退外联
    sum(t.xs_score) as xs_score, -- 线索质量分 需要扩展
    sum(t.first_call_duration) as first_call_duration, -- 首次通话时长 需要扩展
    sum(t.first_call_duration_num) as first_call_duration_num, -- 首次通话时数量 需要扩展
    sum(t.online_dk_cnt) as online_dk_cnt,  -- 线上带看量
    sum(t.400_xs_cnt)   as 400_xs_cnt,   -- 400线索量  
    sum(t.400_sh_cnt) as 400_sh_cnt,     -- 400上户量 
    sum(t.intent_low_num) as intent_low_num, -- 当日上户关闭订单
  	sum(t.call_duration_sh_num) as call_duration_sh_num,-- 通话上户数量
    null                        as developer_xs_cnt -- 开发商线索数量
from tmp_etl.tmp_market_full_process_yw_04 t -- 纬度数据
group by 
   t.report_date,
   t.city_name,
   t.media_type,
   t.product_type,
   t.device_type,
   t.from_source,
   t.if_refer
union all
-- 2.开发商线索量
select 
t.create_date             as report_date,
t.city_name, --业务城市
t.media_name              as media_type,   -- 媒体名称
t.module_name             as product_type, -- 模块名称
t.device_name             as device_type, -- 设备名称
 -1                       as from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
 null                     as if_refer, -- 是否友介 1:是，0否
 null                     as show_num,-- 展示
 null 										as click_num,-- 点击
 null                     as bill_cost,-- 帐面消耗
 null                     as cost,--消耗
 null                     as xs_cnt, --线索量
 null                     as sh_cnt, --上户量
 null                     as dk_cnt, --带看量
 null                     as rg_cnt, --认购量
 null                     as qy_cnt, --签约量
 null                     as rengou_yingshou, --认购应收含退外联
 null                     as rengou_yingshou_net,--认购应收净
 null                     as qianyue_yingshou,--签约应收含退外联
 null                     as xs_score, -- 线索质量分 需要扩展
 null                     as first_call_duration, -- 首次通话时长 需要扩展
 null                     as first_call_duration_num, -- 首次通话时数量 需要扩展
 null                     as online_dk_cnt, -- 线上带看量
 null                     as 400_xs_cnt,   -- 400线索量  
 null                     as 400_sh_cnt,
 null                     as intent_low_num,-- 当日上户关闭订单
 null                     as call_duration_sh_num, -- 通话上户数量
 count(1)                 as developer_xs_cnt -- 开发
from julive_fact.fact_kfsclue_full_line_indi t
group by 
        t.create_date,
        t.city_name,    --业务城市
        t.media_name,   -- 媒体名称
        t.module_name,  -- 模块名称
        t.device_name -- 设备名称
union all   
--- 3.关联展示、点击、消耗（sem、app部分）
select 
t.report_date as report_date,
t.city_name, --业务城市
t.media_type,   -- 媒体名称
t.product_type, -- 模块名称
t.device_type,  -- 设备名称
0 											 as from_source,  -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
null                     as if_refer,     -- 是否友介 1:是，0否
sum(t.show_num)          as show_num,
sum(t.click_num)         as click_num,
sum(t.bill_cost)         as bill_cost,
sum(t.cost)              as cost,
 null                    as xs_cnt, --线索量
 null                    as sh_cnt, --上户量
 null                    as dk_cnt, --带看量
 null                    as rg_cnt, --认购量
 null                    as qy_cnt, --签约量
 null                    as rengou_yingshou, --认购应收含退外联
 null                    as rengou_yingshou_net,--认购应收净
 null                    as qianyue_yingshou,--签约应收含退外联
 null                    as xs_score, -- 线索质量分 需要扩展
 null                    as first_call_duration, -- 首次通话时长 需要扩展
 null                    as first_call_duration_num, -- 首次通话时数量 需要扩展
 null                    as online_dk_cnt, -- 线上带看量
 null                    as 400_xs_cnt,   -- 400线索量  
 null                    as 400_sh_cnt,
 null                    as intent_low_num,-- 当日上户关闭订单
 null                    as call_duration_sh_num, -- 通话上户数量
 null                    as developer_xs_cnt -- 开发商线索数量
 from tmp_etl.tmp_market_full_process_yw_05 t -- 消耗表
GROUP BY
        report_date,
        device_type,
        media_type,
        product_type,
        city_name
union all
--- 4.关联展示、点击、消耗（非sem、非app部分）
select 
p.report_date as report_date,
-- p.city_name, --业务城市
regexp_extract(p.city_name, '(.*?)(市|$)', 1) as city_name, --业务城市
case when p.device_name ='APP渠道' and p.report_date >= '2019-11-25' and t.media_type_name = '腾讯智汇推' then '广点通' else t.media_type_name end media_type,   -- 媒体名称
case when p.device_name = 'APP渠道' then p.device_name 
     when p.device_name = '微信小程序' then t.product_type_name else p.media_class end product_type,
case when p.device_name = '微信小程序' then p.device_name else p.app_type end as device_type,  -- 设备名称
0 											 as from_source,  -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
null                     as if_refer,     -- 是否友介 1:是，0否
sum(p.show_num)          as show_num,
sum(p.click_num)         as click_num,
sum(p.bill_cost)         as bill_cost,
sum(p.cost)              as cost,
 null                    as xs_cnt, --线索量
 null                    as sh_cnt, --上户量
 null                    as dk_cnt, --带看量
 null                    as rg_cnt, --认购量
 null                    as qy_cnt, --签约量
 null                    as rengou_yingshou, --认购应收含退外联
 null                    as rengou_yingshou_net,--认购应收净
 null                    as qianyue_yingshou,--签约应收含退外联
 null                    as xs_score, -- 线索质量分 需要扩展
 null                    as first_call_duration, -- 首次通话时长 需要扩展
 null                    as first_call_duration_num, -- 首次通话时数量 需要扩展
 null                    as online_dk_cnt, -- 线上带看量
 null                    as 400_xs_cnt,   -- 400线索量  
 null                    as 400_sh_cnt,
 null                    as intent_low_num,-- 当日上户关闭订单
 null                    as call_duration_sh_num, -- 通话上户数量
 null                    as developer_xs_cnt -- 开发商线索数量
from julive_dim.dim_dsp_account_history t -- 账户表
inner join julive_fact.fact_market_area_report_dtl p -- 消耗表
on t.id=p.account_id
and t.p_date=p.report_date
where (p.source = 'SEM' and p.device_name = 'APP渠道') 
   or p.source <> 'SEM'
 group by 
 			p.report_date,
 			-- p.city_name, --业务城市
 			regexp_extract(p.city_name, '(.*?)(市|$)', 1),-- 业务城市
     case when p.device_name ='APP渠道' and p.report_date >= '2019-11-25' and t.media_type_name = '腾讯智汇推' then '广点通' else t.media_type_name end,
     case when p.device_name = 'APP渠道' then p.device_name 
          when p.device_name = '微信小程序' then t.product_type_name else p.media_class end,
     case when p.device_name = '微信小程序' then p.device_name else p.app_type end
)xx
left join julive_dim.dim_city yy -- 城市维度表
on xx.city_name = yy.city_name
group by 
    xx.report_date,
    xx.city_name,-- 业务城市名称
    yy.region,-- 所属大区
    yy.city_type_first, -- 新老城（主城含副城）
    yy.mgr_city,-- 主城名称
    case when xx.city_name in('北京' ,'上海' ,'天津' ,'广州' ,'深圳' ,'杭州' ,'苏州' ,'成都','重庆' ,'郑州' ,'南京' ,'佛山') then '老城' else '新城' end,
    xx.media_type,
    xx.product_type,
    xx.device_type,
    xx.from_source,
    coalesce(xx.if_refer,0) 
   ;
	
-----六.插入到李宁表
-- insert overwrite table tmp_etl.tmp_market_full_process_yw_06
insert overwrite table tmp_bi.market_full_process_yw
select 
  report_date             ,-- 时间                  
  city_name as city_yw    ,-- 业务城市                
  device_type             ,-- 设备                  
  media_type              ,-- 媒体                  
  product_type            ,-- 投放类型                
  show_num                ,-- 展示                  
  click_num               ,-- 点击                  
  bill_cost               ,-- 账面消耗                
  cost                    ,-- 现金消耗                
  xs_cnt                  ,-- 线索量                 
  sh_cnt                  ,-- 上户量                 
  dk_cnt                  ,-- 带看量                 
  rg_cnt                  ,-- 认购量                 
  qy_cnt                  ,-- 签约量                 
  rengou_yingshou         ,-- 认购应收                
  rengou_yingshou_net     ,-- 认购应收净               
  qianyue_yingshou        ,-- 签约应收                
  '' probs                 ,-- 上户质量分               
  city_new_old            ,--                     
  '' city_group              ,--                     
  mgr_city as zhufucity_dabao,--                     
  region                  ,-- 中华区域                    
  city_type               ,-- 新老城(主城含副城)                    
  developer_xs_cnt        ,-- 开发商线索量                    
  call_duration_sh_num as jietong_sh_day,--                     
  xs_score                ,--                     
  first_call_duration     ,--                     
  intent_low_num          ,--                     
  xs_all_cnt as xs_cnt_all,-- 线索总量(包含yw_order_kfs)                    
  first_call_duration_num ,-- 首次通话时数量                    
  online_dk_cnt           ,-- 线上带看量                    
  400_xs_cnt              ,-- 400线索量                   
  400_sh_cnt              ,-- 400上户量                    
  from_source as yw_line  ,-- 业务线 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据                    
  mgr_city                ,-- 主城                    
  if_refer                 -- 是否友介 1:是，0否
from julive_app.app_market_full_process_yw t;







