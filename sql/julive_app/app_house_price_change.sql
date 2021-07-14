set hive.execution.engine=spark;
set spark.app.name=app_house_price_change;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

--降价户型，降价金额
with house_price_history as(
select 
t3.house_id,
t3.price_min,
t3.down_pay,
t3.project_id,
t3.create_date,
row_number() over(partition by t3.house_id order by t3.create_date desc) as create_datern
  from
  
    (select 
    t2.house_type_id as house_id,
    t2.price as price_min,
    t2.down_pay, 
    t2.project_id,
    t2.create_date,
    row_number() over(partition by t2.house_type_id,t2.price order by t2.create_date desc ) as pricern
      from
      (
      select
      t1.house_type_id,
      t1.price,
      cast(t1.down_pay as int ) as down_pay, 
      t1.project_id,
      t1.create_date,
      row_number() over(partition by t1.house_type_id,t1.create_date order by t1.price) as rn  --过滤脏数据
      from
        (select        
        house_type_id,
        cast(price as int ) as price,
        case
        when length(regexp_extract(down_pay,'([0-9]+\\.)([0-9]+)(E-*[0-9]+)',2))=0 then down_pay
        when length(regexp_extract(down_pay,'([0-9]+\\.)([0-9]+)(E[0-9]+)',2))<=cast(regexp_extract(down_pay,'(E)([0-9]+)',2) as int)
        then rpad(regexp_replace(regexp_extract(down_pay,'([^E]+)',1),'\\.',''),cast(regexp_extract(down_pay,'(E)([0-9]+)',2) as int)+1,'0')
        when length(regexp_extract(down_pay,'([0-9]+\\.)([0-9]+)(E[0-9]+)',2))>cast(regexp_extract(down_pay,'(E)([0-9]+)',2) as int)
        then concat(substr(regexp_replace(regexp_extract(down_pay,'([^E]+)',1),'\\.',''),1,cast(regexp_extract(down_pay,'(E)([0-9]+)',2) as int)+1),'\.',
        substr(regexp_replace(regexp_extract(down_pay,'([^E]+)',1),'\\.',''),cast(regexp_extract(down_pay,'(E)([0-9]+)',2) as int)+2))
        when down_pay regexp 'E-'
        then concat('0.',repeat('0',cast(regexp_extract(down_pay,'(E)(-)([0-9]+)',3) as int)-1),regexp_replace(regexp_extract(down_pay,'(.+)(E)',1),'\\.',''))
        else down_pay
        end as down_pay, 
        project_id,
        from_unixtime(create_datetime,'yyyy-MM-dd') as create_date
          from
          ods.cj_house_type_history

        )t1
        where t1.price>0
    )t2
    where t2.rn =1) t3
  where t3.pricern =1
),

house_first_newest_price as
(
select 
t1.house_id,
t1.price_min,
t1.down_pay,
t1.project_id,
t1.create_date,
t1.create_datern
from 
house_price_history t1 
join  julive_dim.dim_house_info t2 on t1.house_id = t2.house_id
where t2.status !=3
and t1.create_datern=1
),
house_second_newest_price as(
select 
house_id,
price_min,
down_pay,
project_id,
create_date,
create_datern
from 
house_price_history
where create_datern=2
)


insert overwrite table julive_app.app_house_price_change

select 
t1.house_id     as house_id,
t1.project_id   as project_id,
t1.price_min    as final_price,
t2.price_min    as last_but_one_price,
t1.down_pay     as final_down_pay,
t2.down_pay     as last_but_one_down_pay,
t1.create_date  as final_create_date,
t2.create_date  as last_but_one_create_date,
case 
when t1.price_min <t2.price_min then 1
when t1.price_min >t2.price_min then 2
else 0
end                             as price_status,
abs(t2.price_min-t1.price_min)  as cut_price

from 
house_first_newest_price t1
join house_second_newest_price t2 on t1.house_id = t2.house_id 
;
