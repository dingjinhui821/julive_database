set mapreduce.job.name=app_mgr_sign_y_grade_report;
set mapreduce.job.queuename=root.etl;
with tmp_mgr_city as (---主管当月归属城市

select
e1.yearmonth,
e1.emp_id,
e1.emp_name,
e1.city_id,
e1.rn
from 
(select
 s2.yearmonth,
 s1.emp_id,
 s1.emp_name,
 s1.city_id,
 count(1) as num,
 row_number()over(partition by s2.yearmonth,s1.emp_id order by count(1) desc) as rn
from julive_dim.dim_ps_employee_info s1
left join julive_dim.dim_date s2
on s1.pdate=s2.date_id
where s1.job_status =1
 and s1.post_name="咨询主管"
group by 
 s2.yearmonth,
 s1.emp_id,
 s1.emp_name,
 s1.city_id
 )e1
 where e1.rn=1
),
tmp_emp_num as (---主管组内咨询师人数-在职非雨燕

select 
c1.yearmonth,
c1.direct_leader_id,
c1.city_id,
c1.emp_num
from 
(select
a2.yearmonth,
a1.direct_leader_id,
a1.city_id,
count(distinct a1.emp_id) as emp_num
from julive_dim.dim_ps_employee_info a1
left join julive_dim.dim_date a2
on a1.pdate=a2.date_id
where  a1.job_status =1
 and datediff(concat(substr(a1.pdate,1,4),'-',substr(a1.pdate,5,2),'-',substr(a1.pdate,7,2)),a1.entry_date)>14
 and post_name="咨询师"
group by 
a2.yearmonth,
a1.direct_leader_id,
a1.city_id
) c1
left join tmp_mgr_city c2
on c1.yearmonth=c2.yearmonth and c1.direct_leader_id=c2.emp_id and c1.city_id=c2.city_id
where c2.rn=1
),
tmp_city_emp_num as(---城市组内平均咨询师人数

select 
city_id,
yearmonth,
avg(emp_num) as city_avg_emp_num
from tmp_emp_num
where emp_num is not null
group by
city_id,
yearmonth
),
tmp_mgr_sign_income as (--主管核算签约应收-含退含外联

select
b5.city_id                                          as mgr_city_id,
b5.city_name                                        as mgr_city_name,
b5.city_seq                                         as mgr_city_seq,
b4.emp_mgr_id                                       as emp_mgr_id,
b4.yearmonth                                        as yearmonth,
b4.adjust_sign_contains_cancel_ext_income           as adjust_sign_contains_cancel_ext_income
from
(select
b3.mgr_city as city,
b1.emp_mgr_id,
b2.yearmonth,
sum(b1.adjust_sign_contains_cancel_ext_income) as adjust_sign_contains_cancel_ext_income
from julive_fact.fact_zxs_adjust_cust_sign_dtl b1
left join julive_dim.dim_date b2
on b1.sign_date=b2.date_str
left join julive_dim.dim_city b3
on b1.mgr_adjust_city_id=b3.city_id
group by
b3.mgr_city,
b1.emp_mgr_id,
b2.yearmonth
) b4
left join julive_dim.dim_city b5
on b4.city=b5.city_name
),
tmp_mgr_subscribe_income as (--主管核算认购应收-含退含外联

select
b5.city_id                                          as mgr_city_id,
b5.city_name                                        as mgr_city_name,
b5.city_seq                                         as mgr_city_seq,
b4.emp_mgr_id                                       as emp_mgr_id,
b4.yearmonth                                        as yearmonth,
b4.adjust_subscribe_contains_cancel_ext_income      as adjust_subscribe_contains_cancel_ext_income
from
(select
b3.mgr_city as city,
b1.emp_mgr_id,
b2.yearmonth,
sum(b1.adjust_subscribe_contains_cancel_ext_income) as adjust_subscribe_contains_cancel_ext_income
from julive_fact.fact_zxs_adjust_cust_subscribe_dtl b1
left join julive_dim.dim_date b2
on b1.subscribe_date=b2.date_str
left join julive_dim.dim_city b3
on b1.mgr_adjust_city_id=b3.city_id
group by
b3.mgr_city,
b1.emp_mgr_id,
b2.yearmonth
) b4
left join julive_dim.dim_city b5
on b4.city=b5.city_name
),
tmp_mgr_sign_subscribe_income as (

select
coalesce(a.mgr_city_id,b.mgr_city_id)                                         as mgr_city_id,
coalesce(a.mgr_city_name,b.mgr_city_name)                                     as mgr_city_name,
coalesce(a.mgr_city_seq,b.mgr_city_seq)                                       as mgr_city_seq,
coalesce(a.emp_mgr_id,b.emp_mgr_id)                                           as emp_mgr_id,
coalesce(a.yearmonth,b.yearmonth)                                             as yearmonth,
coalesce(a.adjust_sign_contains_cancel_ext_income,0)                          as adjust_sign_contains_cancel_ext_income,
coalesce(b.adjust_subscribe_contains_cancel_ext_income,0)                     as adjust_subscribe_contains_cancel_ext_income
from tmp_mgr_sign_income a
full join tmp_mgr_subscribe_income b
on a.mgr_city_id=b.mgr_city_id and a.emp_mgr_id=b.emp_mgr_id and a.yearmonth=b.yearmonth
),

tmp_city_y_price as (---城市y值
select
d4.city_id,
d4.yearmonth,
d4.y_value
from 
(select
d3.city_id as city_id,
d3.yearmonth as yearmonth,
d3.y_value as y_value,
d3.num,
row_number() over( partition by d3.city_id,d3.yearmonth   order by d3.num desc) as rn
from 
(
select
d1.adjust_city_id as city_id,
d2.yearmonth as yearmonth,
d1.y_val as y_value,
count(1) as num
from ods.yw_sign_bonus_director d1
left join julive_dim.dim_date d2 on from_unixtime(d1.adjust_datetime,"yyyy-MM-dd")=d2.date_str
group by
d1.adjust_city_id,
d2.yearmonth,
d1.y_val
) d3
)d4
where  d4.rn=1 

)

INSERT overwrite table julive_app.app_mgr_sign_y_grade_report

select 
t1.emp_mgr_id,
t7.employee_name,

t1.mgr_city_id as adjust_mgr_city_id, ---业绩核算城市
t1.mgr_city_name as adjust_mgr_city_name,
t1.mgr_city_seq as adjust_mgr_city_seq,

t5.city_id, ---主管当月组织架构所属城市
t6.city_name,
t6.city_seq,

t1.adjust_sign_contains_cancel_ext_income,  ----主管核算签约应收
t1.adjust_subscribe_contains_cancel_ext_income,  ----主管核算认购应收
coalesce(t2.emp_num,t3.city_avg_emp_num) as emp_num_stand,  ----咨询师人数处理得
t4.y_value,  ---城市y值
(t1.adjust_sign_contains_cancel_ext_income/coalesce(t2.emp_num,t3.city_avg_emp_num)/t4.y_value) as mgr_y_scores,
(t1.adjust_subscribe_contains_cancel_ext_income/coalesce(t2.emp_num,t3.city_avg_emp_num)/t4.y_value) as mgr_sub_y_scores,

t1.yearmonth,
current_timestamp()             as etl_time 
from tmp_mgr_sign_subscribe_income t1 
left join tmp_emp_num t2 on  t1.mgr_city_id=t2.city_id and t1.emp_mgr_id=t2.direct_leader_id and t1.yearmonth=t2.yearmonth
left join tmp_city_emp_num  t3 on  t1.mgr_city_id=t3.city_id and t1.yearmonth=t3.yearmonth
left join tmp_city_y_price t4  on  t1.mgr_city_id=t4.city_id and t1.yearmonth=t4.yearmonth
left join tmp_mgr_city t5 on       t1.emp_mgr_id=t5.emp_id and t1.yearmonth=t5.yearmonth
left join julive_dim.dim_city t6 on t5.city_id=t6.city_id
left join ods.yw_employee t7 on t1.emp_mgr_id=t7.id
where t1.mgr_city_id is not null and t1.yearmonth>="2019-01-01";
