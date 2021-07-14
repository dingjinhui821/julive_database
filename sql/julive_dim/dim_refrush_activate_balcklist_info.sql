
--测试
insert overwrite table julive_dim.dim_refrush_activate_balcklist_info partition(pdate='20210513')
select  
t3.p_ip,
t3.p_model,
t3.comjia_unique_id

from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,comjia_unique_id
from julive_fact.fact_event_dtl
where  pdate='20210513' 
and event='AppInstall'
and(utm_source='' or utm_source is null)
and get_json_object(properties,"$.channel")='appstore') t3
join 
(select t1.pdate as pdate ,t1.p_ip as p_ip ,t1.p_model as p_model
from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,count(distinct comjia_unique_id) as cnt
from julive_fact.fact_event_dtl
where  pdate='20210513' 
and event='AppInstall'
and (utm_source='' or utm_source is null)
and get_json_object(properties,"$.channel")='appstore'
group by pdate,p_ip,get_json_object(properties,"$.model") 
) t1
where t1.cnt>3
)t2  on t2.pdate=t3.pdate and t2.p_ip=t3.p_ip and t2.p_model=t3.p_model;


--上线


insert overwrite table julive_dim.dim_refrush_activate_balcklist_info partition(pdate)
select  
t3.p_ip                              as p_ip,
t3.p_model                           as p_model,
t3.comjia_unique_id                  as comjia_unique_id,
${hiveconf:etl_date}                 as pdate
from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,comjia_unique_id
from tmp_dev_1.tmp_event_json_parsed
where  pdate=${hiveconf:etl_date} 
and event='AppInstall'
and(product_name='' or product_name is null)
and get_json_object(properties,"$.channel")='appstore') t3
join 
(select t1.pdate as pdate ,t1.p_ip as p_ip ,t1.p_model as p_model
from 
(select pdate,p_ip,get_json_object(properties,"$.model") as p_model,count(distinct comjia_unique_id) as cnt
from tmp_dev_1.tmp_event_json_parsed
where  pdate=${hiveconf:etl_date} 
and event='AppInstall'
and (product_name='' or product_name is null)
and get_json_object(properties,"$.channel")='appstore'
group by pdate,p_ip,get_json_object(properties,"$.model") 
) t1
where t1.cnt>3
)t2  on t2.pdate=t3.pdate and t2.p_ip=t3.p_ip and t2.p_model=t3.p_model
;

