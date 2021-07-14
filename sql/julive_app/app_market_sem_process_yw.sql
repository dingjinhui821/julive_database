--  功能描述：市场各环节数据（业务城市）,每天全量重跑，分为12:00/14:00 各跑一次，依赖线上消耗表和线下已经计算好的临时表                          
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
--  输 出 表：julive_app.app_market_sem_process_yw                      -- 市场SEM投放数据(数仓业务城市版)表
--           tmp_bi.market_sem_process_yw                              -- 李宁-市场SEM投放数据(数仓业务城市版)表
--
--  创 建 者：  姜宝桥  18600505375  jiangbaoqiao@julive.com
--  创建日期： 2021/07/06 15:16
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容

-- set hive.execution.engine=spark;
set spark.app.name=app_market_sem_process_yw;
set mapred.job.name=app_market_sem_process_yw;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=3;
set spark.executor.memory=8g;
set spark.executor.instances=14;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


-- 临时表参考hera作业：app_market_full_process_yw
-- 一.创建临时表，通话次数、通话时长处理（忽略-已处理）tmp_etl.tmp_market_full_process_yw_01
-- 二.明细层，线下各个流程节点明细数据（忽略-已处理）tmp_etl.tmp_market_full_process_yw_02
-- 三.修复媒体名称、设备类型（忽略-已处理）tmp_etl.tmp_market_full_process_yw_03
-- 四.业务城市渠道处理（忽略-已处理）tmp_etl.tmp_market_full_process_yw_04

insert overwrite table julive_app.app_market_sem_process_yw
select
    xx.report_date, -- 报告日期
    xx.city_name,-- 业务城市名称
    xx.media_type, -- 媒体名称：是修复后的数据
    xx.product_type, -- 模块名称
    xx.device_type, --设备名称：是修复后的数据
    xx.channel_id,   -- 渠道id
    xx.from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    yy.mgr_city,-- 主城名称
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
--- 1.自营部分（SEM部分）
select 
    t.report_date             as report_date,
    t.city_name               as city_name,-- 业务城市
    t.media_type              as media_type,  -- 媒体名称：是修复后的数据
    t.product_type            as product_type, -- 模块名称
    t.device_type             as device_type,-- 设备名称：是修复后的数据
    t.from_source             as from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    t.channel_id              as channel_id,   -- 渠道id
    null                      as show_num,-- 展示
    null 										  as click_num,-- 点击
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
where product_type='SEM'
group by 
    t.report_date,
    t.city_name,
    t.media_type,
    t.product_type,
    t.device_type,
    t.from_source,
    t.channel_id    
union all
-- 2.开发商线索量（SEM部分）
select 
    t.create_date             as report_date,
    t.city_name, --业务城市
    t.media_name              as media_type,   -- 媒体名称
    t.module_name             as product_type, -- 模块名称
    t.device_name             as device_type, -- 设备名称
    -1                        as from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    t.channel_id              as channel_id, -- 渠道id 
    null                      as show_num,-- 展示
    null 										  as click_num,-- 点击
    null                      as bill_cost,-- 帐面消耗
    null                      as cost,--消耗
    null                      as xs_cnt, --线索量
    null                      as sh_cnt, --上户量
    null                      as dk_cnt, --带看量
    null                      as rg_cnt, --认购量
    null                      as qy_cnt, --签约量
    null                      as rengou_yingshou, --认购应收含退外联
    null                      as rengou_yingshou_net,--认购应收净
    null                      as qianyue_yingshou,--签约应收含退外联
    null                      as xs_score, -- 线索质量分 需要扩展
    null                      as first_call_duration, -- 首次通话时长 需要扩展
    null                      as first_call_duration_num, -- 首次通话时数量 需要扩展
    null                      as online_dk_cnt, -- 线上带看量
    null                      as 400_xs_cnt,   -- 400线索量  
    null                      as 400_sh_cnt,
    null                      as intent_low_num,-- 当日上户关闭订单
    null                      as call_duration_sh_num, -- 通话上户数量
    count(1)                  as developer_xs_cnt -- 开发
from julive_fact.fact_kfsclue_full_line_indi t
where module_name='SEM'
group by 
     t.create_date,
     t.city_name,    --业务城市
     t.media_name,   -- 媒体名称
     t.module_name,  -- 模块名称
     t.device_name, -- 设备名称
     t.channel_id
union all   
--- 3.关联展示、点击、消耗（sem且非app部分）
select 
     t.report_date as report_date,
     t.city_name, --业务城市
     t.media_type,   -- 媒体名称
     t.product_type, -- 模块名称
     t.device_type,  -- 设备名称
     0 											  as from_source,  -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
     t.channel_id             as channel_id, -- 渠道id 
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
group by
     t.report_date,
     t.device_type,
     t.media_type,
     t.product_type,
     t.city_name,
     t.channel_id
union all
--- 4.关联展示、点击、消耗（sem且app部分）
select 
    p.report_date as report_date,
    regexp_extract(p.city_name, '(.*?)(市|$)', 1) as city_name, --业务城市
    case when p.device_name ='APP渠道' and p.report_date >= '2019-11-25' and t.media_type_name = '腾讯智汇推' then '广点通' else t.media_type_name end media_type,   -- 媒体名称
    case when p.device_name = 'APP渠道' then p.device_name 
         when p.device_name = '微信小程序' then t.product_type_name else p.media_class end product_type,
    case when p.device_name = '微信小程序' then p.device_name else p.app_type end as device_type,  -- 设备名称
    0 											 as from_source,  -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    p.channel_id             as channel_id, -- 渠道id 
    sum(p.show_num)          as show_num,
    sum(p.click_num)         as click_num,
    sum(p.bill_cost)         as bill_cost,
    sum(p.cost)              as cost,
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
    null                     as developer_xs_cnt -- 开发商线索数量
from julive_dim.dim_dsp_account_history t -- 账户表
inner join julive_fact.fact_market_area_report_dtl p -- 消耗表
        on t.id=p.account_id
       and t.p_date=p.report_date
     where (p.source = 'SEM' and p.device_name = 'APP渠道') 
     group by 
     			p.report_date,
     			regexp_extract(p.city_name, '(.*?)(市|$)', 1),-- 业务城市
         case when p.device_name ='APP渠道' and p.report_date >= '2019-11-25' and t.media_type_name = '腾讯智汇推' then '广点通' else t.media_type_name end,
         case when p.device_name = 'APP渠道' then p.device_name 
              when p.device_name = '微信小程序' then t.product_type_name else p.media_class end,
         case when p.device_name = '微信小程序' then p.device_name else p.app_type end,
         p.channel_id
)xx
left join julive_dim.dim_city yy -- 城市维度表
on xx.city_name = yy.city_name
group by 
    xx.report_date, -- 报告日期
    xx.city_name,-- 业务城市名称
    xx.media_type, -- 媒体名称：是修复后的数据
    xx.product_type, -- 模块名称
    xx.device_type, --设备名称：是修复后的数据
    xx.channel_id,   -- 渠道id
    xx.from_source, -- 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据 -1 开发商 0：线上数据
    yy.mgr_city -- 主城名称
   ;
	
	
-----六.插入到李宁表
-- 历史备份
--create  table tmp_bi.market_sem_process_yw_bk20210706 like tmp_bi.market_sem_process_yw;
--insert overwrite table tmp_bi.market_sem_process_yw_bk20210706 select * from tmp_bi.market_sem_process_yw;

DROP VIEW IF EXISTS tmp_bi.market_sem_process_yw;
CREATE VIEW IF NOT EXISTS tmp_bi.market_sem_process_yw (
		report_date                            COMMENT '报告日期',
		channel_id                             COMMENT '渠道id',
		channel_name                           COMMENT '渠道名',
		device_type                            COMMENT '设备',
		media_type                             COMMENT '媒体',
		product_type                           COMMENT '投放类型',
		city_yw                                COMMENT '业务城市',
		show_num                               COMMENT '展示',
		click_num                              COMMENT '点击',
		bill_cost                              COMMENT '账面消耗',
		cost                                   COMMENT '现金消耗',
		xs_cnt                                 COMMENT '线索量',
		sh_cnt                                 COMMENT '上户量',
		dk_cnt                                 COMMENT '带看量',
		rg_cnt                                 COMMENT '认购量',
		qy_cnt                                 COMMENT '签约量',
		rengou_yingshou                        COMMENT '认购应收',
		rengou_yingshou_net                    COMMENT '认购应收',
		qianyue_yingshou                       COMMENT '签约应收净',
		probs                                  COMMENT '上户质量分',
		city_group                             COMMENT '城市组（字段已废弃）',
		zhufucity_dabao                        COMMENT '主城市名称',
		developer_xs_cnt                       COMMENT '开发商线索量',
		jietong_sh_day                         COMMENT '通话上户数量',
		xs_score                               COMMENT '通话得分',
		first_call_duration                    COMMENT '首次通话时长',
		intent_low_num                         COMMENT '当日上户关闭订单',
		xs_cnt_all                             COMMENT '线索总量(包含yw_order_kfs)',
		first_call_duration_num                COMMENT '首次通话时数量',
		online_dk_cnt                          COMMENT '线上带看量',
		400_xs_cnt                             COMMENT '400线索量',
		400_sh_cnt                             COMMENT '400上户量',
		yw_line                                COMMENT '业务线 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据'
)
COMMENT '李宁-市场SEM投放数据(数仓业务城市版)表'
as select 
    t.report_date             as report_date, -- 时间        
    p.channel_id              as channel_id,  -- 渠道id                
    p.channel_name            as channel_name,-- 渠道名  
    t.device_type             as device_type, -- 设备                  
    t.media_type              as media_type,  -- 媒体                  
    t.product_type            as product_type,-- 投放类型
    t.city_name               as city_yw,     -- 业务城市                  
    t.show_num                as show_num,    -- 展示                  
    t.click_num               as click_num,   -- 点击                  
    t.bill_cost               as bill_cost,   -- 账面消耗                
    t.cost                    as cost,        -- 现金消耗                
    t.xs_cnt                  as xs_cnt,      -- 线索量                 
    t.sh_cnt                  as sh_cnt,      -- 上户量                 
    t.dk_cnt                  as dk_cnt,      -- 带看量                 
    t.rg_cnt                  as rg_cnt,      -- 认购量                 
    t.qy_cnt                  as qy_cnt,      -- 签约量                 
    t.rengou_yingshou         as rengou_yingshou,-- 认购应收                
    t.rengou_yingshou_net     as rengou_yingshou_net,-- 认购应收                
    t.qianyue_yingshou        as qianyue_yingshou,-- 签约应收净                
    ''                        as probs,           -- 上户质量分               
    ''                        as city_group,      --                     
    t.mgr_city                as zhufucity_dabao, --                     
    t.developer_xs_cnt        as developer_xs_cnt,-- 开发商线索量                    
    t.call_duration_sh_num    as jietong_sh_day,  --                     
    cast(xs_score as decimal(16,2)) as xs_score,--                     
    t.first_call_duration     as first_call_duration,--                     
    t.intent_low_num          as intent_low_num,--                     
    t.xs_all_cnt              as xs_cnt_all,-- 线索总量(包含yw_order_kfs)                    
    t.first_call_duration_num as first_call_duration_num,-- 首次通话时数量                    
    t.online_dk_cnt           as online_dk_cnt,-- 线上带看量                    
    t.400_xs_cnt              as 400_xs_cnt,-- 400线索量                   
    t.400_sh_cnt              as 400_sh_cnt,-- 400上户量                    
    t.from_source             as yw_line    -- 业务线 1-自营 2-乌鲁木齐 3-二手房中介 4-加盟商数据      
from julive_app.app_market_sem_process_yw t
left join julive_dim.dim_channel_info p 
on cast(t.channel_id as bigint)=p.channel_id
;
