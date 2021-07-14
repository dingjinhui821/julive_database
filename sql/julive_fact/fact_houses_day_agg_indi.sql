
set hive.execution.engine=spark;
set spark.app.name=fact_houses_day_agg_indi;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

with fact_clue_houses_day_indi as(

select 
t2.create_date,
t2.project_id,
count(distinct t2.clue_id) as clue_num 
from(
  select 
  t1.clue_id,
  t1.project_id,
  t1.create_date 
  from ( 
    select 
    clue_id,
    project_id,
    create_date 
    from julive_dim.dim_clue_info 
    lateral view explode(split(interest_project,',')) project_list as project_id 
    ) t1 
  where project_id is not null  
  and project_id != 0
  and project_id != 'null'
  )t2
  group by t2.create_date,t2.project_id  
),
fact_distribute_houses_day_indi as(
select 
t2.create_date,
t2.project_id,
count(distinct t2.clue_id) as distribute_num 
from(
  select 
  t1.clue_id,
  t1.project_id,
  t1.create_date 
  from ( 
    select 
    clue_id,
    project_id,
    create_date 
    from julive_dim.dim_clue_info 
    lateral view explode(split(interest_project,',')) project_list as project_id
    where is_distribute = 1 
    ) t1 
  where project_id is not null
  and project_id != 0 
  and project_id != 'null'
  )t2
  group by t2.create_date,t2.project_id  
),
fact_see_project_houses_day_indi as(
  select 
  t1.project_id,
  sum(t1.see_num) as see_num,
  t1.create_date
  from
    (
    select 
    project_id                                 as project_id,  
    see_project_num                            as see_num,   
    to_date(plan_real_begin_time)              as create_date
    from julive_fact.fact_see_project_dtl)t1
  where project_id is not null 
  and project_id != 0 
  group by t1.create_date,t1.project_id 
),
fact_subscribe_houses_day_indi as(
   select
   t1.project_id,
   sum(t1.subscribe_contains_cancel_ext_num)     as subscribe_contains_cancel_ext_num,
   t1.create_date
   from(
     select 
     project_id                                  as project_id,
     subscribe_contains_cancel_ext_num           as subscribe_contains_cancel_ext_num,
     to_date(subscribe_time)                     as create_date
     from julive_fact.fact_subscribe_dtl 
     )t1
   where project_id is not null 
   and project_id != 0
 
   group by t1.project_id,t1.create_date

),
fact_back_subscribe_houses_day_indi as (
    select 
    t1.project_id,
    sum(t1.subscribe_cancel_contains_ext_num)       as subscribe_cancel_contains_ext_num ,
    t1.create_date
    from
        (select 
        project_id                                  as project_id,
        subscribe_cancel_contains_ext_num           as subscribe_cancel_contains_ext_num,
        subscribe_time                              as create_date
        from julive_fact.fact_subscribe_dtl)t1
    where project_id is not null 
    
    and project_id != 0
    
    group by t1.project_id,t1.create_date
),
fact_sign_houses_day_indi as(
    select
    t1.project_id,
    sum(t1.sign_contains_cancel_ext_num)            as sign_contains_cancel_ext_num,
    t1.create_date
    from      
        (select
        project_id                                  as project_id,
        sign_contains_cancel_ext_num                as sign_contains_cancel_ext_num,
        subscribe_date                              as create_date
        from julive_fact.fact_sign_dtl) t1
    where project_id is not null 
 
    and project_id != 0
  
    group by t1.project_id,t1.create_date
)


insert overwrite table julive_fact.fact_houses_day_agg_indi

select 
t11.project_id,
t11.project_name,
t11.project_city_id,
t11.project_city_name,
t11.project_city_seq,
if(t11.clue_num is null,0,t11.clue_num)                as clue_num,
if(t11.distribute_num is null,0,t11.distribute_num)    as distribute_num,
if(t11.see_num is null ,0,t11.see_num)                 as see_num,
if(t11.subscribe_contains_cancel_ext_num is null,0,t11.subscribe_contains_cancel_ext_num) as subscribe_contains_cancel_ext_num,
if(t11.subscribe_cancel_contains_ext_num is null,0,t11.subscribe_cancel_contains_ext_num) as subscribe_cancel_contains_ext_num,
if(t11.sign_contains_cancel_ext_num is null ,0,t11.sign_contains_cancel_ext_num)          as sign_contains_cancel_ext_num ,
t11.create_date,
current_timestamp()   as etl_time

from 
(select 

t10.project_id                         as project_id,
t7.project_name                        as project_name,
t7.city_id                             as project_city_id,
t7.city_name                           as project_city_name,
t8.city_seq                            as project_city_seq,
t10.clue_num                           as clue_num,
t10.distribute_num                     as distribute_num,
t10.see_num                            as see_num,
t10.subscribe_contains_cancel_ext_num  as subscribe_contains_cancel_ext_num,
t10.subscribe_cancel_contains_ext_num  as subscribe_cancel_contains_ext_num,
t10.sign_contains_cancel_ext_num       as sign_contains_cancel_ext_num,
t10.create_date                        as create_date,
row_number() over(partition by t10.project_id,t10.create_date order by clue_num desc) as rn
from
(select
t9.project_id                             as project_id,
sum(t9.clue_num)                          as clue_num,
sum(t9.distribute_num)                    as distribute_num,
sum(t9.see_num)                           as see_num,
sum(t9.subscribe_contains_cancel_ext_num) as subscribe_contains_cancel_ext_num,   
sum(t9.subscribe_cancel_contains_ext_num) as subscribe_cancel_contains_ext_num,   
sum(t9.sign_contains_cancel_ext_num)      as sign_contains_cancel_ext_num, 
t9.create_date                            as create_date
from
(
select 
coalesce(t1.project_id,t2.project_id,t3.project_id,t4.project_id,t5.project_id,t6.project_id)                       
                               as project_id, 

t1.clue_num,
t2.distribute_num,
t3.see_num,
t4.subscribe_contains_cancel_ext_num,   
t5.subscribe_cancel_contains_ext_num,   
t6.sign_contains_cancel_ext_num, 
coalesce(t1.create_date,t2.create_date,t3.create_date,t4.create_date,t5.create_date,t6.create_date)
                               as create_date

from  fact_clue_houses_day_indi t1   
full join fact_distribute_houses_day_indi t2     on t1.project_id =t2.project_id and t1.create_date = t2.create_date 
full join fact_see_project_houses_day_indi t3    on t1.project_id =t3.project_id and t1.create_date = t3.create_date 
full join fact_subscribe_houses_day_indi t4      on t1.project_id =t4.project_id and t1.create_date = t4.create_date 
full join fact_back_subscribe_houses_day_indi t5 on t1.project_id =t5.project_id and t1.create_date = t5.create_date 
full join fact_sign_houses_day_indi t6           on t1.project_id =t6.project_id and t1.create_date = t6.create_date
) t9

group by t9.project_id,t9.create_date)t10

left join julive_dim.dim_project_info t7         on t10.project_id = t7.project_id  
left join julive_dim.dim_city t8                 on t7.city_id = t8.city_id)t11

where t11.rn = 1
; 
 






