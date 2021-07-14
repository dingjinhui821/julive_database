
set hive.execution.engine=spark;
set spark.app.name=fact_grass_sign_dtl;
set mapred.job.name=fact_grass_sign_dtl;
set spark.yarn.queue=etl;
insert overwrite table julive_fact.fact_grass_sign_base_dtl

select

t1.id                                                  as sign_id,
t1.order_id                                            as clue_id,
t3.channel_id                                          as channel_id,
t1.deal_id                                             as deal_id,
t1.subscribe_id                                        as subscribe_id,
t1.employee_id                                         as emp_id,
''                                                     as emp_name,
t1.user_id                                             as user_id,
t1.sign_name                                           as user_name,
t3.customer_intent_city_id                             as customer_intent_city_id,-- 20191015 
t3.customer_intent_city_name                           as customer_intent_city_name,-- 20191015 
t3.customer_intent_city_seq                            as customer_intent_city_seq,-- 20191015 
t4.city_id                                             as project_city_id,
t4.city_name                                           as project_city_name,
t5.city_seq                                            as city_seq,
t1.project_id                                          as project_id,
t1.project_name                                        as project_name,
t1.city_id                                             as city_id,
t2.city_name                                           as city_name,
t2.city_seq                                            as city_seq,
t1.house_type                                          as house_type,
t1.acreage                                             as acreage,
t1.house_number                                        as house_number,
                                                       
t1.status                                              as sign_status,
t1.sign_type                                           as sign_type,
                                                       
t1.deal_money                                          as orig_deal_amt,
if(t1.status = 3,1,0)                                  as orig_grass_num,
if(t1.status in (3,4),1,0)                             as orig_grass_contains_cancel_num,

-- 量 
if(t1.status = 3 and t1.sign_type in (1),1,0)          as grass_coop_num,
if(t1.status = 3 and t1.sign_type in (1,4),1,0)        as grass_contains_ext_num,
if(t1.status = 4 and t1.sign_type in (1),1,0)          as grass_coop_cancel_num,
if(t1.status = 4 and t1.sign_type in (1,4),1,0)        as grass_contains_ext_cancel_num,
                                                       
from_unixtime(t1.grass_sign_datetime)                  as grass_sign_time,
from_unixtime(t1.create_datetime)                      as create_time,
-- 20210316 线索总表中包含以下信息
case
when t3.from_source is not null then t3.from_source
else 1 
end                                                    as from_source,
t3.org_id                                              as org_id,
t3.org_name                                            as org_name,
current_timestamp()                                    as etl_time

from ods.yw_sign_grass t1
-- left join ods.cj_district t2 on t1.city_id = t2.id 
left join julive_dim.dim_city t2 on t1.city_id = t2.city_id
--20210316 dim_clue_info（自营） 换为dim_clue_base_info(线索总表)
left join julive_dim.dim_clue_base_info t3 on t1.order_id = t3.clue_id
left join julive_dim.dim_project_info t4 on t1.project_id = t4.project_id and t4.end_date = '9999-12-31'
left join julive_dim.dim_city t5 on t4.city_id = t5.city_id
where t1.status != -1
;


-- 加工子表 


-- 居理数据 
insert overwrite table julive_fact.fact_grass_sign_dtl 
select t.* 
from julive_fact.fact_grass_sign_base_dtl t 
where t.from_source = 1 ;

-- 乌鲁木齐数据 
insert overwrite table julive_fact.fact_wlmq_grass_sign_dtl  
select t.* 
from julive_fact.fact_grass_sign_base_dtl t 
where t.from_source = 2 ;

-- 二手房中介数据 
insert overwrite table julive_fact.fact_esf_grass_sign_dtl
select t.* 
from julive_fact.fact_grass_sign_base_dtl t 
where t.from_source = 3 ;

-- 加盟商数据 
insert overwrite table julive_fact.fact_jms_grass_sign_dtl 
select t.* 
from julive_fact.fact_grass_sign_base_dtl t 
where t.from_source = 4 ;

insert into table julive_fact.fact_jms_grass_sign_dtl 
select t.* 
from julive_fact.fact_grass_sign_base_dtl t 
where t.from_source = 1 and t.org_id != 48;

