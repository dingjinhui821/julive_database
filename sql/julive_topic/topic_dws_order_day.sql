set hive.execution.engine=spark;
set spark.app.name=topic_dws_order_day;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=12g;
set spark.executor.instances=6;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=3000;
set hive.exec.max.dynamic.partitions.pernode=3000;

set etl_date = '${hiveconf:etlDate}';

-- 线索
with 
tmp_clue as (
    select 
        create_date,
        clue_id,
        sum(1) as clue_num,
        sum(if(source = 9 OR source = 10, 1 ,0 )) as clue_num_400
    from julive_dim.dim_clue_base_info
    where create_date >= date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-300)
    group by create_date,
        clue_id
),
-- 上户 
tmp_distribute as (
    select 
        distribute_date,
        clue_id,
        sum(1) as distribute_num,
        sum(if(source = 9 OR source = 10, 1 ,0 )) as distribute_num_400
    from julive_dim.dim_clue_base_info
    where is_distribute = 1
        and distribute_date >= date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-300)
    group by distribute_date,
        clue_id
),
-- 带看
tmp_see as (  
    select 
        to_date(plan_real_begin_time) as see_date,
        clue_id,
        count(distinct if(see_num>0,see_id,null)) as see_num,
        count(distinct if(see_type=2 and see_num>0,see_id,null)) as see_num_online
    from julive_fact.fact_see_project_base_dtl 
    where see_create_date >= date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-300)
    group by to_date(plan_real_begin_time),
        clue_id
), 
-- 认购
tmp_subscribe as(
    select 
        to_date(subscribe_time) as subscribe_date,
        clue_id,
        sum(subscribe_contains_cancel_ext_num) as subscribe_num,
        sum(subscribe_contains_ext_num) as subscribe_contains_ext_num,
        sum(subscribe_coop_num) as subscribe_coop_num,
        sum(subscribe_contains_cancel_ext_income) as subscribe_contains_cancel_ext_income,
        sum(subscribe_contains_ext_income) as subscribe_contains_ext_income,
        sum(subscribe_contains_cancel_ext_amt) as subscribe_contains_cancel_ext_amt,
        sum(subscribe_contains_ext_amt) as subscribe_contains_ext_amt
    from julive_fact.fact_subscribe_base_dtl 
    where to_date(subscribe_time) >= date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-300)
    group by to_date(subscribe_time),clue_id
),  
    -- 签约
tmp_sign as(
    select
        to_date(sign_time) as sign_date,
        clue_id,
        sum(sign_contains_cancel_ext_num)  as sign_num,
        sum(if(sign_type in (1,4) AND sign_status in ('1','2'),orig_sign_income,0)) as sign_income_orig
    from julive_fact.fact_sign_base_dtl 
    where to_date(sign_time) >= date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-300)
    group by to_date(sign_time) ,
        clue_id 
        
)

insert overwrite TABLE julive_topic.topic_dws_order_day partition (pdate)
select 
    coalesce(t1.clue_id,t2.clue_id,t3.clue_id,t4.clue_id,t5.clue_id) as clue_id,
    coalesce(t1.create_date,t2.distribute_date,t3.see_date,t4.subscribe_date,t5.sign_date) as report_date,
    t1.clue_num,
    t1.clue_num_400,
    t2.distribute_num,
    t2.distribute_num_400,
    t3.see_num,
    t3.see_num_online,
    t4.subscribe_num,
    t4.subscribe_contains_ext_num,
    t4.subscribe_coop_num,
    t4.subscribe_contains_cancel_ext_income,
    t4.subscribe_contains_ext_income,
    t4.subscribe_contains_cancel_ext_amt,
    t4.subscribe_contains_ext_amt,
    t5.sign_num,
    t5.sign_income_orig,
    current_timestamp() as etl_time,
    date_format(coalesce(t1.create_date,t2.distribute_date,t3.see_date,t4.subscribe_date,t5.sign_date),'yyyyMMdd') as pdate
from tmp_clue t1
full join tmp_distribute t2 on t1.create_date = t2.distribute_date and t1.clue_id = t2.clue_id
full join tmp_see t3 on coalesce(t1.create_date,t2.distribute_date) = t3.see_date and coalesce(t1.clue_id,t2.clue_id) = t3.clue_id
full join tmp_subscribe t4 on  coalesce(t1.create_date,t2.distribute_date,t3.see_date) = t4.subscribe_date
          and coalesce(t1.clue_id,t2.clue_id,t3.clue_id) = t4.clue_id
full join tmp_sign t5 on  coalesce(t1.create_date,t2.distribute_date,t3.see_date,t4.subscribe_date) = t5.sign_date
          and coalesce(t1.clue_id,t2.clue_id,t3.clue_id,t4.clue_id) = t5.clue_id
;