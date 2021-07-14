set hive.execution.engine=spark;
set spark.app.name=app_mgr_manage_grade_report;
set spark.yarn.queue=etl;

with tmp_mgr_city as (---主管当月归属城市

select
e1.yearmonth                                   as yearmonth,
e1.emp_id                                      as mgr_id,
e1.emp_name                                    as mgr_name,
e1.city_id                                     as city_id,
e1.city_name                                   as city_name,
e1.city_seq                                    as city_seq  
from 
(select
 s2.yearmonth,
 s1.emp_id,
 s1.emp_name,
 s1.city_id,
 s1.city_name,
 s3.city_seq,
 count(1) as num,
 row_number()over(partition by s2.yearmonth,s1.emp_id order by count(1) desc) as rn
from julive_dim.dim_ps_employee_info s1
left join julive_dim.dim_date s2
on s1.pdate=s2.date_id
left join julive_dim.dim_city s3
on  s1.city_id=s3.city_id
where s1.job_status =1
 and s1.post_name="咨询主管"
group by 
 s2.yearmonth,
 s1.emp_id,
 s1.emp_name,
 s1.city_id,
 s1.city_name,
 s3.city_seq
 )e1
 where e1.rn=1
),
tmp_mgr_loss_scores as (---主管流失分

select
a4.team_leader_id                 as mgr_id,
a4.offjob_month                   as offjob_month,
a5.city_id                        as city_id,
sum(a4.man_power)                 as loss_man_power
from
(
select
a3.id,
a3.team_leader_id,
a3.employee_id,
a3.adjust_date,
a3.offjob_date,
dim_date.yearmonth as offjob_month,
((year(a3.offjob_date)-year(a3.adjust_date))*12+(month(a3.offjob_date)-month(a3.adjust_date))) as month_diff,
man_power
from
(
select
a1.id,
a1.team_leader_id,
a1.employee_id,
from_unixtime(a1.adjust_datetime,"yyyy-MM-dd") as adjust_date,
a2.offjob_date,
a1.man_power
from ods.yw_manpower_director a1
left join julive_dim.dim_ps_employee_info a2  
on a1.employee_id=a2.emp_id 
and a2.pdate=regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd"),-1)),'-','')
) a3
left join julive_dim.dim_date on a3.offjob_date=dim_date.date_str
)a4
left join tmp_mgr_city a5
on a4.team_leader_id=a5.mgr_id and a4.offjob_month=a5.yearmonth
where a4.month_diff<=3
group by a4.team_leader_id,a4.offjob_month,a5.city_id
),
tmp_city_loss_scores as (---城市流失分

select
offjob_month                       as offjob_month,
city_id                            as city_id,
avg(loss_man_power)                as loss_man_power_city
from tmp_mgr_loss_scores
group by
offjob_month,
city_id
),
tmp_person_coefficient_scores as (---人头系数分

select
p5.employee_id                     as mgr_id,
p5.adjust_month                    as adjust_month,
p5.mgr_city                        as city_name,
p6.city_id                         as city_id,
p5.head_ratio                      as head_ratio
from
(select
p1.employee_id as employee_id,
from_unixtime(p1.adjust_datetime,"yyyy-MM") as adjust_month,
p2.mgr_city,
sum(p1.head_ratio) as head_ratio
from ods.yw_sign_bonus_director p1
left join julive_dim.dim_city p2
on p1.adjust_city_id=p2.city_id
group by 
from_unixtime(p1.adjust_datetime,"yyyy-MM"),
p1.employee_id,
p2.mgr_city
) p5
left join 
(select
p3.emp_id,
p3.emp_name,
p3.city_id,
p3.city_name,
p4.date_str,
p4.yearmonth,
row_number()over(partition by p3.emp_id,p4.yearmonth order by p4.date_str desc) as rn
from julive_dim.dim_ps_employee_info p3
left join julive_dim.dim_date p4
on p3.pdate=p4.date_id
where p3.job_status =1
 and p3.post_name="咨询主管"
) p6
on p5.employee_id=p6.emp_id and p5.adjust_month=p6.yearmonth and p5.mgr_city=p6.city_name and p6.rn=1
where p6.city_id is not null
),
tmp_high_educated_rate as (--高学历详情表

 select 
 b2.emp_id                                      as emp_id,
 b2.emp_name                                    as emp_name,
 b2.direct_leader_id                            as mgr_id,
 b2.direct_leader_name                          as mgr_name,
 b2.city_id                                     as city_id,
 b2.city_name                                   as city_name,
 b1.educated                                    as educated,
 b3.yearmonth                                   as yearmonth,
 if(b1.educated="高学历",1,0)                   as educated_num
 from julive_dim.dim_ps_employee_info b2
 left join 
 (select 
  skey,
  date_id,
  yearmonth,
  row_number()over(partition by yearmonth_zh order by date_of_month desc) as rn
 from julive_dim.dim_date
 )b3
 on b2.pdate=b3.date_id
 left join
(select
 aa.emp_id,
 aa.emp_name,
 aa.school_attributes,
 aa.school_type,
 case 
 when school_type in ("985","211") then "高学历"
 when school_type like "%海外%" then "高学历"
 when school_type="一本" then "一本"
 when school_type is null then null
 else "二本及以下" end as educated
 from
(select 
 emp_id,
 emp_name,
 school_attributes,
 case 
 when school_attributes=3 then "一本"
 when school_attributes=4 then "二本"
 when school_attributes=1 then "985"
 when school_attributes=2 then "211"
 when school_attributes=5 then "三本"
 when school_attributes=7 then "海外院校"
 when school_attributes=6 then "专科及以下"
 else school_attributes end as school_type
 from julive_dim.dim_ps_employee_info
 where pdate=regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd"),-1)),'-','')
) aa
) b1  --员工最新状态
 on b2.emp_id=b1.emp_id
 where b2.job_status=1 and b2.post_name="咨询师" and b3.rn=1----取一个月的最后一天
),
tmp_mgr_high_educated_rate as(---主管高学历占比

select 
mgr_id                                               as mgr_id ,
city_id                                              as city_id,
yearmonth                                            as yearmonth,
(sum(educated_num)/count(distinct emp_id))*100       as mgr_high_educated_rate
from tmp_high_educated_rate
group by
mgr_id,
city_id,
yearmonth
),
tmp_city_high_educated_rate as(---城市高学历占比

select 
city_id                                             as city_id,
yearmonth                                           as yearmonth,
(sum(educated_num)/count(distinct emp_id))*100          as city_high_educated_rate
from tmp_high_educated_rate
group by
city_id,
yearmonth
),
tmp_mgr_hatch_scores as(---主管孵化分

select
c6.team_leader_id                         as mgr_id,
c6.promo_month                            as promo_month,
sum(c6.man_power)                         as promo_man_power
from 
(select
c5.id,
c5.team_leader_id,
c5.employee_id,
from_unixtime(c5.adjust_datetime,"yyyy-MM-dd") as adjust_date,
c4.promo_month,
c4.promo_date,
((year(c4.promo_date)-year(from_unixtime(c5.adjust_datetime,"yyyy-MM-dd")))*12+(month(c4.promo_date)-month(from_unixtime(c5.adjust_datetime,"yyyy-MM-dd")))) as month_diff,
c5.man_power
from ods.yw_manpower_director c5
left join
(select
c3.emp_id,
c3.date_str as promo_date, ---主管晋升时间
c3.yearmonth as promo_month
from 
(select
 c1.emp_id,
 c2.date_str,
 c2.yearmonth,
 row_number()over(partition by c1.emp_id order by c1.pdate asc) as rn
from julive_dim.dim_ps_employee_info c1
left join julive_dim.dim_date c2
on c1.pdate=c2.date_id
where c1.post_name="咨询主管"
) c3
where c3.rn=1
)c4
on c5.employee_id=c4.emp_id
)c6
where c6.month_diff<=3
group by 
c6.team_leader_id,
c6.promo_month
),
tmp_high_rank_promote_scores as (---高职级晋升分

select
d8.team_leader_id                                           as mgr_id,
d8.promote_month                                            as promote_month,
substr(add_months(concat(d8.promote_month,"-01"),2),1,7)    as promote_end_month,
sum(d8.man_power)                                           as promote_man_power
from
(select
d7.id,
d7.team_leader_id,
d7.employee_id,
from_unixtime(d7.adjust_datetime,"yyyy-MM") as adjust_month,
d6.promote_month,
((year(concat(d6.promote_month,"-01"))-year(from_unixtime(d7.adjust_datetime,"yyyy-MM-dd")))*12+(month(concat(d6.promote_month,"-01"))-month(from_unixtime(d7.adjust_datetime,"yyyy-MM-dd")))) as month_diff,
d7.man_power
from ods.yw_manpower_director d7
left join
(select
d5.employee_id,
d5.yearmonth as promote_month,
d5.year_id,
d5.qr_id,
d5.quarter_level,
d5.last_quarter_level,
"1" as promote_num
from
(select
d3.employee_id,
d3.yearmonth,
d3.year_id,
d3.qr_id,
d3.quarter_level,
d4.quarter_level as last_quarter_level
from
(select
d1.employee_id,
d2.yearmonth,
d2.year_id,
d2.qr_id,
d1.quarter_level,
case
when (d2.month_no-3)<0 then concat((d2.year_id-1),"-",(d2.month_no+9))
when (d2.month_no-3)>0 then concat(d2.year_id,"-","0",(d2.month_no-3))
else null end as last_qr
from 
(select 
employee_id,
from_unixtime(adjust_datetime,"yyyy-MM") as adjust_month,
quarter_level,
count(id)
from ods.yw_employee_grade_level
where from_unixtime(adjust_datetime,"yyyy-MM")>="2014-01"
group by 
employee_id,
from_unixtime(adjust_datetime,"yyyy-MM"),
quarter_level
)d1
left join
(select
yearmonth,
month_no,
year_id,
qr_id,
count(1),
row_number()over(partition by year_id,qr_id order by month_no asc) as rn
from julive_dim.dim_date
group by
yearmonth,
month_no,
year_id,
qr_id
) d2
on d1.adjust_month=d2.yearmonth
where d2.rn=1
)d3
left join 
(select 
employee_id,
from_unixtime(adjust_datetime,"yyyy-MM") as adjust_month,
quarter_level,
count(id)
from ods.yw_employee_grade_level
where from_unixtime(adjust_datetime,"yyyy-MM")>="2014-01"
group by 
employee_id,
from_unixtime(adjust_datetime,"yyyy-MM"),
quarter_level
)d4
on d3.last_qr=d4.adjust_month and d3.employee_id=d4.employee_id
) d5
where d5.last_quarter_level is not null and d5.quarter_level>=5 and(d5.quarter_level-d5.last_quarter_level)>0
)d6
on d7.employee_id=d6.employee_id
)d8
where d8.month_diff>=1 and d8.month_diff<=3
group by
d8.team_leader_id,
d8.promote_month
)

INSERT overwrite table julive_app.app_mgr_manage_grade_report

select

t1.mgr_id                                                                   as mgr_id,
t1.mgr_name                                                                 as mgr_name,
t1.city_id                                                                  as city_id,
t1.city_name                                                                as city_name,
t1.city_seq                                                                 as city_seq,  
                                                                           
coalesce(t2.loss_man_power,0)                                               as loss_man_power,
coalesce(t3.loss_man_power_city,0)                                          as loss_man_power_city,
coalesce((t2.loss_man_power/t3.loss_man_power_city),0)                      as loss_standard_scores,

coalesce(t4.head_ratio,0)                                                   as person_coefficient_scores,

coalesce(t5.mgr_high_educated_rate,0)                                       as mgr_high_educated_rate,
coalesce(t6.city_high_educated_rate,0)                                      as city_high_educated_rate,
coalesce((t5.mgr_high_educated_rate/t6.city_high_educated_rate),0)          as high_educated_scores,

coalesce(t7.promo_man_power,0)                                              as hatch_scores,
coalesce(t8.promote_man_power,0)                                            as high_rank_promote_scores,

t1.yearmonth                                                           as yearmonth,
current_timestamp()                                                    as etl_time

from tmp_mgr_city t1
left join tmp_mgr_loss_scores t2 on t1.mgr_id=t2.mgr_id and t1.city_id=t2.city_id and t1.yearmonth=t2.offjob_month
left join tmp_city_loss_scores t3 on t1.city_id=t3.city_id and t1.yearmonth=t3.offjob_month
left join tmp_person_coefficient_scores  t4 on t1.mgr_id=t4.mgr_id and t1.city_id=t4.city_id and t1.yearmonth=t4.adjust_month
left join tmp_mgr_high_educated_rate t5 on t1.mgr_id=t5.mgr_id and t1.city_id=t5.city_id and t1.yearmonth=t5.yearmonth
left join tmp_city_high_educated_rate t6 on t1.city_id=t6.city_id and t1.yearmonth=t6.yearmonth
left join tmp_mgr_hatch_scores t7 on t1.mgr_id=t7.mgr_id  and t1.yearmonth=t7.promo_month
left join tmp_high_rank_promote_scores t8 on t1.mgr_id=t8.mgr_id  and t1.yearmonth=t8.promote_month
where t1.yearmonth>="2019-01";
