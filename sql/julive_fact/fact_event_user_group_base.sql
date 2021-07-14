
set etl_date = '${hiveconf:etldate}';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- test 

set hive.execution.engine=spark;
set spark.app.name=fact_event_user_group_base;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table tmp_dev_1.tmp_fact_event_user_group_add_first_abtest partition(pdate)

select 

t1.global_id,
${hiveconf:etl_date} as pdate 

from (

select t.global_id
from julive_fact.fact_event_dtl t
where t.pdate = ${hiveconf:etl_date}
  and t.event in ('view_p_home_all_abtest','e_page_view_abtest')
group by t.global_id

) t1 left semi join (

select t.global_id 
from julive_fact.fact_event_dtl t 
where t.pdate = ${hiveconf:etl_date}
  and t.event = 'e_page_view'
  and t.p_is_first_day = 'true'
group by t.global_id

) t2 on t1.global_id = t2.global_id
;


insert overwrite table julive_fact.fact_event_user_group_base partition(pdate)

select 

t1.skey,
t1.session_id,
t1.prev_event_elapse,
t1.next_event_elapse,
t1.user_access_seq_asc_today,
t1.user_access_seq_desc_today,
t1.global_id,

t1.user_id,
t1.julive_id,
t1.comjia_unique_id,
t1.cookie_id,
t1.idfa,
t1.idfv,
t1.p_ip,
t1.comjia_imei,
t1.fl_project_id,
t1.fl_project_name,
t1.product_id,
t1.product_name,
t1.event,
t1.app_version,
t1.p_app_version,
t1.abtest_name,
t1.abtest_value,
t1.select_city,
t1.frompage,
t1.toPage,
t1.fromModule,
t1.tomodule,
t1.leave_phone_state,
t1.user_agent,
t1.order_id,
t1.is_new_order,
t1.current_url,
t1.to_url,
t1.referer,
t1.op_type,
t1.source,
t1.channel_id,
t1.channel_put,
t1.login_state,
t1.p_utm_source,
t1.button_title,
t1.operation_type,
t1.operation_position,
t1.banner_id,
t1.tab_id,

case 
when t1.p_is_first_day = 'true' then 1 
when t1.p_is_first_day = 'false' then 0 
else 0 end                               as p_is_first_day,      -- 20190910 添加字段 :首次访问用户 
if(t2.global_id is not null,1,0)         as abtest_is_first_day, -- 20190910 添加字段 :是否首次参与实验 

t1.create_time,
t1.pplatform,

-- 审计及分区字段 
current_timestamp() as etl_time,
${hiveconf:etl_date} as pdate

from julive_fact.fact_event_dtl t1 
left join tmp_dev_1.tmp_fact_event_user_group_add_first_abtest t2 on t1.global_id = t2.global_id and t2.pdate = ${hiveconf:etl_date}

where t1.pdate = ${hiveconf:etl_date} 
;

