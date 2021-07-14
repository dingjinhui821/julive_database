set hive.execution.engine=spark;
set spark.app.name=topic_dws_order_day;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=12;

set etl_date = '${hiveconf:etlDate}';

--   功能描述：TOPIC-DWT-订单业务指标表

--   输 入 表 ：
--         julive_fact.fact_clue_full_line_base_indi        -- 线下订单粒度及后续转化明细指标表
--         julive_dim.dim_clue_base_info                    -- 线索维度基表-包含多条业务线数据 from_source区分
--         julive_fact.fact_market_order_rel_appinstall     -- 订单关联AppInstall表
--         dm.sh_quality_order_score                        -- 订单上户质量分
--         julive_fact.fact_consultant_call_dtl             -- 咨询师通话明细

--   输 出 表：julive_topic.topic_dwt_order_base
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/23 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容


with 
tmp_call as 
(
    select 
        a.clue_id,
        sum(if(is_first_called = 1,a.call_duration,0)) as first_call_duration,
        sum(if(distribute_date = to_date(release_time),a.call_duration,0)) as distribute_call_duration,
        sum(a.call_duration) as call_duration,
        count(1) as call_num
    from julive_fact.fact_consultant_call_dtl a 
    where a.call_duration > 0
    group by a.clue_id
),
tmp_probs as 
(
    select
        order_id as clue_id,
        sum(probs)/count(order_id) as distribute_probs -- 预测的上户质量分
    from dm.sh_quality_order_score
    group by order_id
),

tmp_dim_clue as
(
    SELECT 
        clue_id,
        clue_score, -- 线索质量得分 
        if(intent = 1 and datediff(to_date(intent_low_time),distribute_date) < 1,1,0) as intent_low_num
    FROM julive_dim.dim_clue_base_info
),

tmp_install as
(
    SELECT 
        order_id as clue_id,
        to_date(install_date_time) as install_date,
        install_date_time AS install_time
    FROM julive_fact.fact_market_order_rel_appinstall 
)


insert overwrite TABLE julive_topic.topic_dwt_order_base
select 
    t1.clue_id,
    t5.install_date,
    t5.install_time,
    t1.create_date,
    t1.create_time,
    t1.distribute_date,
    t1.distribute_time,
    t1.first_see_date,
    t1.final_see_date,
    t1.first_subscribe_date,
    t1.final_subscribe_date,
    t1.first_sign_date,
    t1.final_sign_date,
    t1.clue_num,
    t1.distribute_num,
    t1.see_num,
    t1.see_project_num,
    t1.subscribe_num,
    t1.subscribe_coop_num,
    t1.subscribe_contains_cancel_ext_num,
    t1.subscribe_contains_ext_num,
    t1.subscribe_contains_ext_amt,
    t1.subscribe_contains_ext_income,
    t1.subscribe_contains_cancel_ext_amt,
    t1.subscribe_contains_cancel_ext_income,
    t1.sign_num,
    t2.call_duration,
    t2.call_num                              ,
    t2.first_call_duration                   ,
    t2.distribute_call_duration,
    t1.clue_id_list                          ,
    t1.distribute_id_list                    ,
    t1.see_id_list                           ,
    t1.see_project_id_list                   ,
    t1.clue_see_list                         ,
    t1.subscribe_contains_cancel_ext_id_list ,
    t1.clue_subscribe_list                   ,
    t1.sign_contains_cancel_ext_id_list      ,
    t1.clue_sign_list                        ,
    t1.clue_see_num                          ,
    t1.clue_subscribe_num                    ,
    t1.clue_sign_num                         ,
    t3.distribute_probs                      ,
    t4.clue_score                            ,
    t4.intent_low_num ,
    current_timestamp() as etl_time
from julive_fact.fact_clue_full_line_base_indi t1
left join tmp_call t2 on t1.clue_id = t2.clue_id
left join tmp_probs t3 on t1.clue_id = t3.clue_id
left join tmp_dim_clue t4 on t1.clue_id = t4.clue_id
left join tmp_install t5 on t1.clue_id = t5.clue_id
;
