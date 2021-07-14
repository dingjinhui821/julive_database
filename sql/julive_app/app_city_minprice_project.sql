set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_city_minprice_project;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

with project_price as (
select
t2.project_id,
min(t2.price_min ) as price_min
from
(
select 
t1.project_id,
cast(t1.price_min as int) as price_min
from 
(select 
case
    when length(regexp_extract(price_min,'([0-9]+\\.)([0-9]+)(E-*[0-9]+)',2))=0 then price_min
    when length(regexp_extract(price_min,'([0-9]+\\.)([0-9]+)(E[0-9]+)',2))<=cast(regexp_extract(price_min,'(E)([0-9]+)',2) as int)
    then rpad(regexp_replace(regexp_extract(price_min,'([^E]+)',1),'\\.',''),cast(regexp_extract(price_min,'(E)([0-9]+)',2) as int)+1,'0')
    when length(regexp_extract(price_min,'([0-9]+\\.)([0-9]+)(E[0-9]+)',2))>cast(regexp_extract(price_min,'(E)([0-9]+)',2) as int)
    then concat(substr(regexp_replace(regexp_extract(price_min,'([^E]+)',1),'\\.',''),1,cast(regexp_extract(price_min,'(E)([0-9]+)',2) as int)+1),'\.',
    substr(regexp_replace(regexp_extract(price_min,'([^E]+)',1),'\\.',''),cast(regexp_extract(price_min,'(E)([0-9]+)',2) as int)+2))
    when price_min regexp 'E-'
    then concat('0.',repeat('0',cast(regexp_extract(price_min,'(E)(-)([0-9]+)',3) as int)-1),regexp_replace(regexp_extract(price_min,'(.+)(E)',1),'\\.',''))
    else price_min
    end as price_min, 
project_id
from
julive_app.cj_project_cut_price 
where pdate = ${hiveconf:etl_date}
) t1 ) t2
where t2.price_min>0
group by t2.project_id
),
projet_valid_project as(
select t1.* 
from project_price t1 
join ods.cj_project t2 on t1.project_id = t2.project_id  
and t2.status!= 3
and t2.is_show=1
and t2.project_type in(1,2)
)

insert overwrite table julive_app.app_city_minprice_project

select 
tmp.project_id,
tmp.project_name,
CEILING(tmp.price_min/10000) as price_min,
tmp.project_city,
tmp.district_id,
tmp.district_name
from
(select 
t1.project_id,
t2.name        as project_name,
t2.city_id     as project_city,
t1.price_min,
t2.district_id,
t2.district_name,
row_number() over(partition by t2.city_id order by t1.price_min) as rn
from projet_valid_project t1
join ods.cj_project t2 on t1.project_id = t2.project_id  
where t2.district_id NOT IN ('20000032','20000034','20000033','20000031','20000030') 
)tmp 
where tmp.rn <=5;


