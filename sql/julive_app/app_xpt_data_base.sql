
set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_xpt_data_base;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table julive_app.app_xpt_data_base partition(pdate = ${hiveconf:etl_date})

select

t1.global_id,
t1.comjia_unique_id,
case
when t1.event = 'e_click_confirm_leave_phone' then '5'
when t1.event = 'e_page_view' then '2'
else '0' end as object,

case
when t1.event = 'e_click_confirm_leave_phone' then '7'
when t1.event = 'e_page_view' then '8'
else '0' end as operation,

case
when t1.product_id = '20' and get_json_object(t1.properties,'$.esf_house_id') <> '0' and get_json_object(t1.properties,'$.esf_house_id') <> '' then get_json_object(t1.properties,'$.esf_house_id')
when t1.product_id = '20' then get_json_object(t1.properties,'$.village_id')
when t1.product_id <> '20' then t1.project_id
else '0' end as target_id,

case
when t1.product_id = '20' and get_json_object(t1.properties,'$.esf_house_id') <> '0' and get_json_object(t1.properties,'$.esf_house_id') <> '' then '2' -- 二手房
when t1.product_id = '20' then '5' -- 小区
when t1.product_id <> '20' then '1' -- 新房
else '0' end as target_type,

get_json_object(t1.properties,'$.comjia_platform_id') as comjia_platform_id,
t1.product_id as product_id,
t1.frompage as frompage,
t1.op_type as optype_id,
get_json_object(t1.properties,'$.store_user_id') as agent_id,
get_json_object(t1.properties,'$.store_login_id') as agent_login_id,
t1.julive_id as customer_id,
t1.comjia_unique_id as openid,
get_json_object(regexp_replace(t1.properties,'\\$','p_'),'$.p_os') as `system`,
get_json_object(regexp_replace(t1.properties,'\\$','p_'),'$.p_network_type') as network,
if(t1.create_timestamp is not null and t1.create_timestamp <> '0' and t1.create_timestamp <> '',cast(create_timestamp/1000 as bigint),unix_timestamp(current_timestamp())) as operate_time

from julive_fact.fact_event_dtl t1 

where pdate = ${hiveconf:etl_date} 
and (
( -- 条件1 新房APP楼盘原生页面
t1.event = 'e_page_view'
and t1.topage = 'p_project_details'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and substr(t1.product_id,1,3) in ('101','201')
) or ( -- 条件2 新房 H5
t1.event = 'e_page_view'
and t1.topage = 'p_project_home'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '2'
and get_json_object(t1.properties,'$.is_new_page') = '1'
) or ( -- 条件3 线上售楼处
t1.event = 'e_page_view'
and t1.topage = 'p_developer_project_details'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '7'
) or ( -- 条件4 二手房H5 -- 二手房详情页 - 不按端过滤 20200722 - 去除 comjia_platform_id 条件
t1.event = 'e_page_view'
and t1.topage = 'p_esf_house_details'
-- and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '20'
-- ) or ( -- 条件5 二手房M站
-- t1.event = 'e_page_view'
-- and t1.topage = 'p_esf_house_details'
-- -- and get_json_object(t1.properties,'$.comjia_platform_id') = '2'
-- and t1.product_id = '20'
) or ( -- 条件6 二手房小区详情页
t1.event = 'e_page_view'
and t1.topage = 'p_esf_village_details'
and t1.product_id = '20'
) or ( -- 条件7 新房APP
t1.event = 'e_click_confirm_leave_phone'
and t1.frompage = 'p_online_im_chat'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and substr(t1.product_id,1,3) in ('101','201')
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
) or ( -- 条件8 二手房H5
t1.event = 'e_click_confirm_leave_phone'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '20'
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
) or ( -- 条件9 新房M站二手房
t1.event = 'e_click_confirm_leave_phone'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') = '2'
and t1.product_id = '20'
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
)
)
;

insert into table julive_app.app_xpt_data_base partition(pdate = ${hiveconf:etl_date})

select

t1.global_id,
t1.comjia_unique_id,
case
when t1.event = 'e_click_confirm_leave_phone' then '5'
when t1.event = 'e_page_view' then '2'
else '0' end as object,

case
when t1.event = 'e_click_confirm_leave_phone' then '7'
when t1.event = 'e_page_view' then '8'
else '0' end as operation,

case
when t1.product_id = '20' and get_json_object(t1.properties,'$.esf_house_id') <> '0' and get_json_object(t1.properties,'$.esf_house_id') <> '' then get_json_object(t1.properties,'$.esf_house_id')
when t1.product_id = '20' then get_json_object(t1.properties,'$.village_id')
when t1.product_id <> '20' then t1.project_id
else '0' end as target_id,

case
when t1.product_id = '20' and get_json_object(t1.properties,'$.esf_house_id') <> '0' and get_json_object(t1.properties,'$.esf_house_id') <> '' then '2' -- 二手房
when t1.product_id = '20' then '5' -- 小区
when t1.product_id <> '20' then '1' -- 新房
else '0' end as target_type,

get_json_object(t1.properties,'$.comjia_platform_id') as comjia_platform_id,
t1.product_id as product_id,
t1.frompage as frompage,
t1.op_type as optype_id,
get_json_object(t1.properties,'$.store_user_id') as agent_id,
get_json_object(t1.properties,'$.store_login_id') as agent_login_id,
t1.julive_id as customer_id,
t1.comjia_unique_id as openid,
get_json_object(regexp_replace(t1.properties,'\\$','p_'),'$.p_os') as `system`,
get_json_object(regexp_replace(t1.properties,'\\$','p_'),'$.p_network_type') as network,
if(t1.create_timestamp is not null and t1.create_timestamp <> '0' and t1.create_timestamp <> '',cast(create_timestamp/1000 as bigint),unix_timestamp(current_timestamp())) as operate_time

from julive_fact.fact_event_esf_dtl t1 

where pdate = ${hiveconf:etl_date} 
and (
( -- 条件1 新房APP楼盘原生页面
t1.event = 'e_page_view'
and t1.topage = 'p_project_details'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and substr(t1.product_id,1,3) in ('101','201')
) or ( -- 条件2 新房 H5
t1.event = 'e_page_view'
and t1.topage = 'p_project_home'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '2'
and get_json_object(t1.properties,'$.is_new_page') = '1'
) or ( -- 条件3 线上售楼处
t1.event = 'e_page_view'
and t1.topage = 'p_developer_project_details'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '7'
) or ( -- 条件4 二手房H5 -- 二手房详情页 - 不按端过滤 20200722 - 去除 comjia_platform_id 条件
t1.event = 'e_page_view'
and t1.topage = 'p_esf_house_details'
-- and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '20'
-- ) or ( -- 条件5 二手房M站
-- t1.event = 'e_page_view'
-- and t1.topage = 'p_esf_house_details'
-- -- and get_json_object(t1.properties,'$.comjia_platform_id') = '2'
-- and t1.product_id = '20'
) or ( -- 条件6 二手房小区详情页
t1.event = 'e_page_view'
and t1.topage = 'p_esf_village_details'
and t1.product_id = '20'
) or ( -- 条件7 新房APP
t1.event = 'e_click_confirm_leave_phone'
and t1.frompage = 'p_online_im_chat'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and substr(t1.product_id,1,3) in ('101','201')
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
) or ( -- 条件8 二手房H5
t1.event = 'e_click_confirm_leave_phone'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') in ('101','201')
and t1.product_id = '20'
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
) or ( -- 条件9 新房M站二手房
t1.event = 'e_click_confirm_leave_phone'
and t1.leave_phone_state = '1'
and get_json_object(t1.properties,'$.comjia_platform_id') = '2'
and t1.product_id = '20'
and get_json_object(t1.properties,'$.business_type') = '3' -- 20200602添加
)
)
;


