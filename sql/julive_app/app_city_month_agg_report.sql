set hive.execution.engine=spark;
set spark.app.name=app_city_month_agg_report;
set spark.yarn.queue=etl;

with tmp_subscribe_project_city as ( -- 破蛋楼盘数 

select 

substr(subscribe_time,1,7) as yearmonth,
project_city_id            as city_id,
count(distinct project_id) as subscribe_contains_cancel_ext_project_num

from julive_fact.fact_subscribe_dtl 
group by 
substr(subscribe_time,1,7),
project_city_id
),
tmp_subscribe_project as (
select 

substr(subscribe_time,1,7) as yearmonth,
count(distinct project_id) as subscribe_contains_cancel_ext_project_num

from julive_fact.fact_subscribe_dtl 
group by 
substr(subscribe_time,1,7)
),
tmp_city_distribute_day_num as ( -- 月-城市上户楼盘量 

select 

substr(distribute_date,1,7) as yearmonth,
customer_intent_city_id as city_id,
count(distinct distribute_date) as distribute_day_num 

from julive_dim.dim_clue_info 
where is_distribute = 1 
and distribute_date >= '2017-01-01' 
group by 
substr(distribute_date,1,7),
customer_intent_city_id 

),
tmp_distribute_day_num as ( -- 月-城市上户楼盘量 

select 

substr(distribute_date,1,7) as yearmonth,
count(distinct distribute_date) as distribute_day_num 

from julive_dim.dim_clue_info 
where is_distribute = 1 
and distribute_date >= '2017-01-01' 
group by 
substr(distribute_date,1,7)

)

insert overwrite table julive_app.app_city_month_agg_report 

select 

t2.yearmonth,
t2.yearmonth_zh,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.city_region,
t1.city_type,

sum(t1.clue_num)                                    as clue_num,
sum(t1.distribute_num)                              as distribute_num,
max(t4.distribute_day_num)                          as distribute_day_num,  
sum(t1.see_num)                                     as see_num,
sum(t1.see_project_num)                             as see_project_num,
sum(t1.subscribe_contains_cancel_ext_num)           as subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)           as subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)        as subscribe_contains_cancel_ext_income,
max(t3.subscribe_contains_cancel_ext_project_num)   as subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)                  as subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)               as subscribe_contains_ext_income,
sum(t1.subscribe_cancel_contains_ext_amt)           as subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_contains_ext_num)                  as subscribe_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_num)           as subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)        as subscribe_cancel_contains_ext_income,
sum(t1.subscribe_coop_num)                          as subscribe_coop_num,
sum(t1.sign_contains_cancel_ext_num)                as sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)             as sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_num)                       as sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)                as sign_cancel_contains_ext_num,
sum(t1.sign_coop_num)                               as sign_coop_num,
sum(t1.actual_amt)                                  as actual_amt,
sum(t1.real_workday_num)                            as real_workday_num,
current_timestamp()                                 as etl_time 

from julive_fact.fact_city_day_agg_indi t1 
join julive_dim.dim_date t2 on t1.date_str = t2.date_str 
left join tmp_subscribe_project_city t3 on t2.yearmonth = t3.yearmonth and t1.city_id = t3.city_id 
left join tmp_city_distribute_day_num t4 on t2.yearmonth = t4.yearmonth and t1.city_id = t4.city_id
where t1.date_str >= '2017-01-01'
and t1.city_id != 0 

group by 
t2.yearmonth,
t2.yearmonth_zh,
t1.city_id,
t1.city_name,
t1.city_seq,
t1.city_region,
t1.city_type

union all 

select 

t2.yearmonth,
t2.yearmonth_zh,
0                                                   as city_id,
"全国"                                              as city_name,
"00全国"                                            as city_seq,
" "                                                 as city_region,
" "                                                 as city_type,

sum(t1.clue_num)                                    as clue_num,
sum(t1.distribute_num)                              as distribute_num,
max(t4.distribute_day_num)                          as distribute_day_num,
sum(t1.see_num)                                     as see_num,
sum(t1.see_project_num)                             as see_project_num,
sum(t1.subscribe_contains_cancel_ext_num)           as subscribe_contains_cancel_ext_num,
sum(t1.subscribe_contains_cancel_ext_amt)           as subscribe_contains_cancel_ext_amt,
sum(t1.subscribe_contains_cancel_ext_income)        as subscribe_contains_cancel_ext_income,
max(t3.subscribe_contains_cancel_ext_project_num)   as subscribe_contains_cancel_ext_project_num,
sum(t1.subscribe_contains_ext_amt)                  as subscribe_contains_ext_amt,
sum(t1.subscribe_contains_ext_income)               as subscribe_contains_ext_income,
sum(t1.subscribe_cancel_contains_ext_amt)           as subscribe_cancel_contains_ext_amt,
sum(t1.subscribe_contains_ext_num)                  as subscribe_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_num)           as subscribe_cancel_contains_ext_num,
sum(t1.subscribe_cancel_contains_ext_income)        as subscribe_cancel_contains_ext_income,
sum(t1.subscribe_coop_num)                          as subscribe_coop_num,
sum(t1.sign_contains_cancel_ext_num)                as sign_contains_cancel_ext_num,
sum(t1.sign_contains_cancel_ext_income)             as sign_contains_cancel_ext_income,
sum(t1.sign_contains_ext_num)                       as sign_contains_ext_num,
sum(t1.sign_cancel_contains_ext_num)                as sign_cancel_contains_ext_num,
sum(t1.sign_coop_num)                               as sign_coop_num,
sum(t1.actual_amt)                                  as actual_amt,
sum(t1.real_workday_num)                            as real_workday_num,
current_timestamp()                                 as etl_time 

from julive_fact.fact_city_day_agg_indi t1 
join julive_dim.dim_date t2 on t1.date_str = t2.date_str 
left join tmp_subscribe_project t3 on t2.yearmonth = t3.yearmonth 
left join tmp_distribute_day_num t4 on t2.yearmonth = t4.yearmonth
where t1.date_str >= '2017-01-01'
group by 
t2.yearmonth,
t2.yearmonth_zh
;
