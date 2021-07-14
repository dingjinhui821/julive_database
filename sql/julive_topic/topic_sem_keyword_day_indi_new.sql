set DATE_ID = ${hiveconf:etlDate};

set spark.app.name=topic_sem_keyword_day_indi;
set hive.execution.engine=spark;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=16g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=16384;


INSERT OVERWRITE TABLE julive_topic.topic_sem_keyword_day_indi partition(pdate,pclue)

SELECT

    keyword.city_id,
    keyword.city_name,
    keyword.media_id,
    keyword.media_name,
    keyword.module_id,
    keyword.module_name,
    keyword.channel_id,
    keyword.channel_name,
    keyword.account_id,
    keyword.account_name,
    keyword.plan_id,
    keyword.plan_name,
    keyword.unit_id,
    keyword.unit_name,
    keyword.keyword_id,
    keyword.keyword_name,
    keyword.match_type,
	keyword.pc_average_ranking,  -- 20191213添加 
	keyword.m_average_ranking, -- 20191213添加 
	keyword.pc_url, -- 20191213添加 
	keyword.m_url, -- 20191213添加 
    keyword.show_num,
    keyword.click_num,
	keyword.bill_cost, -- 20191213添加 
    keyword.cost,
    keyword.leave_phone_num,
    keyword.clue_num,
    keyword.distribute_num,
    keyword.see_num,
    keyword.subscribe_num,
    keyword.sign_num,
    keyword.returned_amt,
    keyword.first_clue_num,
    keyword.first_distribute_num,
    keyword.first_see_num,
    keyword.first_subscribe_num,
    keyword.first_sign_num,
    keyword.first_returned_amt,
    keyword.jump_num,
    keyword.uv,

    cast(effect_word.eff_leave_phone_num as float) as eff_leave_phone_num,
    cast(effect_word.eff_clue_num as float) as eff_clue_num,
    cast(effect_word.eff_dist_num as float) as eff_dist_num,
    cast(effect_word.eff_see_num as float) as eff_see_num,
    cast(effect_word.eff_subscribe_num as float) as eff_subscribe_num,
    cast(effect_word.eff_num as int) as eff_num,

    cast(400_data.uv_400 as int) as uv_400,
    cast(400_data.pv_400 as int) as pv_400,

    cast(current_timestamp() as string) as etl_time,

    keyword.pdate,
    keyword.pclue

FROM (

    SELECT
        city_id,
        city_name,
        media_id,
        media_name,
        module_id,
        module_name,
        channel_id,
        channel_name,
        account_id,
        account_name,
        plan_id,
        plan_name,
        unit_id,
        unit_name,
        keyword_id,
        keyword_name,
        match_type,
		pc_average_ranking,
		m_average_ranking,
		pc_url,
		m_url,
        show_num,
        click_num,
		bill_cost,
        cost,
        leave_phone_num,
        clue_num,
        distribute_num,
        see_num,
        subscribe_num,
        sign_num,
        returned_money as returned_amt,
        first_clue_num,
        first_distribute_num,
        first_see_num,
        first_subscribe_num,
        first_sign_num,
        first_returned_money as first_returned_amt,
        jump_num,
        uv,
        pclue,
        pdate
    FROM tmp_dev_1.tmp_market_dsp_sem_keyword_day_indi 
    WHERE pdate = '${hiveconf:DATE_ID}'

) keyword LEFT JOIN (

    SELECT
        channel_id,
        c_keywordid,
        pdate,
        count(distinct global_id) as uv_400,
        count(1) as pv_400
    FROM
        (
		SELECT
            key_word_null.channel_id,
            key_word_null.channel_put,
            tmp_topic_sem_keyword_day_indi.keyword_id as c_keywordid,
            key_word_null.pdate,
            key_word_null.global_id
        FROM
            (SELECT
                channel_id,
                channel_put,
                get_json_object(properties,'$.c_keywordid') as c_keywordid,
                pdate,
                global_id
            FROM julive_fact.fact_event_dtl 
            WHERE pdate = '${hiveconf:DATE_ID}'
            AND event = 'e_click_dial_service_call'
            AND (
			get_json_object(properties,'$.c_keywordid') is null 
			or get_json_object(properties,'$.c_keywordid') < 1 
			or get_json_object(properties,'$.c_keywordid') = ''
			))key_word_null
            LEFT JOIN
            (SELECT
                channel_id,
                keyword_id,
                concat_ws('|',plan_name,unit_name,keyword_name) as channel_put
            FROM julive_topic.topic_sem_keyword_day_indi
            WHERE pdate = '${hiveconf:DATE_ID}')tmp_topic_sem_keyword_day_indi
            ON key_word_null.channel_id = tmp_topic_sem_keyword_day_indi.channel_id
            AND key_word_null.channel_put = tmp_topic_sem_keyword_day_indi.channel_put
        UNION ALL
        SELECT
            channel_id,
            channel_put,
            get_json_object(properties,'$.c_keywordid') as c_keywordid,
            pdate,
            global_id
        FROM julive_fact.fact_event_dtl 
        WHERE pdate = '${hiveconf:DATE_ID}'
        AND event = 'e_click_dial_service_call'
        AND get_json_object(properties,'$.c_keywordid') is not null 
		and get_json_object(properties,'$.c_keywordid') > 1 
		and get_json_object(properties,'$.c_keywordid') != ''
    )process1
    GROUP BY
        channel_id,
        c_keywordid,
        pdate
		
) 400_data ON  keyword.channel_id = 400_data.channel_id AND keyword.keyword_id = 400_data.c_keywordid AND keyword.pdate = 400_data.pdate 

LEFT JOIN (

SELECT
        keyword_id,
        channel_id_z,
        pdate,
        sum(leave_phone_cnt) as eff_leave_phone_num,
        sum(clue_cnt) as eff_clue_num,
        sum(dist_cnt) as eff_dist_num,
        sum(seeproj_cnt) as eff_see_num,
        sum(subs_cnt) as eff_subscribe_num,
        count(1) as eff_num 
    FROM dm.dm_flo_assist_effect_d
    WHERE pdate = '${hiveconf:DATE_ID}'
    GROUP BY
        keyword_id,
        channel_id_z,
        pdate

) effect_word ON  keyword.channel_id = effect_word.channel_id_z AND keyword.keyword_id = effect_word.keyword_id AND keyword.pdate = effect_word.pdate
;


-- 将存在线索结果集插入到临时表 
insert overwrite table tmp_dev_1.topic_sem_keyword_day_indi_apy_tbl_tmp partition(pdate) 

select 

t.city_id,
t.city_name,
t.media_id,
t.media_name,
t.module_id,
t.module_name,
t.channel_id,
t.channel_name,
t.account_id,
t.account_name,
t.plan_id,
t.plan_name,
t.unit_id,
t.unit_name,
t.keyword_id,
t.keyword_name,
t.match_type,
t.pc_average_ranking,
t.m_average_ranking,
t.pc_url,
t.m_url,
t.show_num,
t.click_num,
t.bill_cost,
t.cost,
t.leave_phone_num,
t.clue_num,
t.distribute_num,
t.see_num,
t.subscribe_num,
t.sign_num,
t.returned_amt,
t.first_clue_num,
t.first_distribute_num,
t.first_see_num,
t.first_subscribe_num,
t.first_sign_num,
t.first_returned_amt,
t.jump_num,
t.uv,
t.eff_leave_phone_num,
t.eff_clue_num,
t.eff_distribute_num,
t.eff_see_num,
t.eff_subscribe_num,
t.eff_num,
t.uv_400,
t.pv_400,
t.etl_time,
t.pdate 

from julive_topic.topic_sem_keyword_day_indi t 
where t.pdate = '${hiveconf:DATE_ID}' 
and t.pclue = 'hasclue'
;

-- 刷新历史数据任务 
insert overwrite table julive_topic.topic_sem_keyword_day_indi partition(pdate,pclue)

select 

coalesce(t1.city_id,t2.city_id,t3.city_id)                                           as city_id, -- 城市 
coalesce(t1.city_name,t2.city_name,t3.city_name)                                     as city_name, 
coalesce(t1.media_id,t2.media_id,t3.media_id)                                        as media_id, -- 媒体 
coalesce(t1.media_name,t2.media_name,t3.media_name)                                  as media_name, -- 媒体名称 
coalesce(t1.module_id,t2.module_id,t3.module_id)                                     as module_id, -- 产品形态 
coalesce(t1.module_name,t2.module_name,t3.module_name)                               as module_name,
coalesce(t1.channel_id,t2.channel_id,t3.channel_id)                                  as channel_id, -- 渠道 
coalesce(t1.channel_name,t2.channel_name,t3.channel_name)                            as channel_name,
coalesce(t1.account_id,t2.account_id,t3.account_id)                                  as account_id, -- 账户 
coalesce(t1.account_name,t2.account_name,t3.account_name)                            as account_name,
coalesce(t1.plan_id,t2.plan_id,t3.plan_id)                                           as plan_id, -- 计划 
coalesce(t1.plan_name,t2.plan_name,t3.plan_name)                                     as plan_name,
coalesce(t1.unit_id,t2.unit_id,t3.unit_id)                                           as unit_id, -- 单元 
coalesce(t1.unit_name,t2.unit_name,t3.unit_name)                                     as unit_name,
coalesce(t1.keyword_id,t2.keyword_id,t3.keyword_id)                                  as keyword_id, -- 关键词 
coalesce(t1.keyword_name,t2.keyword_name,t3.keyword_name)                            as keyword_name,
t1.match_type                                                                        as match_type,

t1.pc_average_ranking                                                                as pc_average_ranking,
t1.m_average_ranking                                                                 as m_average_ranking,
t1.pc_url                                                                            as pc_url,
t1.m_url                                                                             as m_url,

t1.show_num                                                                          as show_num, -- 展 
t1.click_num                                                                         as click_num, -- 点 
t1.bill_cost                                                                         as bill_cost,
t1.cost                                                                              as cost, -- 消 
t1.leave_phone_num                                                                   as leave_phone_num, -- 留电量 

coalesce(t1.clue_num,t2.clue_num,t3.clue_num)                                        as clue_num, -- 线索量 
coalesce(t2.distribute_num,t3.distribute_num,t1.distribute_num)                      as distribute_num, -- 上户量 
coalesce(t2.see_num,t3.see_num,t1.see_num)                                           as see_num, -- 带看量 
coalesce(t2.subscribe_num,t3.subscribe_num,t1.subscribe_num)                         as subscribe_num, -- 认购量 
coalesce(t2.sign_num,t3.sign_num,t1.sign_num)                                        as sign_num, -- 签约量 
null                                                                                 as returned_amt, -- 回款 

coalesce(t1.first_clue_num,t2.first_clue_num,t3.first_clue_num)                      as first_clue_num, -- 首次线索量 
coalesce(t2.first_distribute_num,t3.first_distribute_num,t1.first_distribute_num)    as first_distribute_num, -- 首次上户量 
coalesce(t2.first_see_num,t3.first_see_num,t1.first_see_num)                         as first_see_num, -- 首次带看量 
coalesce(t2.first_subscribe_num,t3.first_subscribe_num,t1.first_subscribe_num)       as first_subscribe_num, -- 首次认购量 
coalesce(t2.first_sign_num,t3.first_sign_num,t1.first_sign_num)                      as first_sign_num, -- 首次签约量 
null                                                                                 as first_returned_amt, -- 首次回款

t1.jump_num                                                                          as jump_num, -- 跳出量 
t1.uv                                                                                as uv, -- UV 

t1.eff_leave_phone_num                                                               as eff_leave_phone_num,
t1.eff_clue_num                                                                      as eff_clue_num,
t1.eff_distribute_num                                                                as eff_distribute_num,
t1.eff_see_num                                                                       as eff_see_num,
t1.eff_subscribe_num                                                                 as eff_subscribe_num,
t1.eff_num                                                                           as eff_num,
t1.uv_400                                                                            as uv_400,
t1.pv_400                                                                            as pv_400,

current_timestamp()                                                                  as etl_time,
t1.pdate                                                                             as pdate,
'hasclue'                                                                            as pclue 

from (

select t.* 
from tmp_dev_1.topic_sem_keyword_day_indi_apy_tbl_tmp t 
where pdate >= '20190702'

) t1 left join (

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi  
where has_keyword_id = 1 -- 存在关键词 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t2 on t1.channel_id = t2.channel_id 
    and t1.keyword_id = t2.channel_put2 
    and t1.pdate = t2.pdate 

left join (

select * 
from tmp_dev_1.tmp_market_offline_add_kw_indi 
where has_keyword_id = 0 -- 不存在关键词 
and channel_put2 != '天津-搜索词-广-YD2|青朗园-主词|天津西青青朗园房价'
and channel_put2 != 'YD-通配-广州-1|低价|清远房价走势2019预测'

) t3 on t1.channel_id = t3.channel_id 
    and concat_ws('|',t1.plan_name,t1.unit_name,t1.keyword_name) = t3.channel_put2 
    and t1.pdate = t3.pdate 
;

