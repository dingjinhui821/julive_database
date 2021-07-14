set etl_date = '${hiveconf:etlDate}';

set hive.execution.engine=spark;
set spark.app.name=dim_clue_base_info;
set mapred.job.name=dim_clue_base_info;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
--  功能描述：线索纬度表,每天全量重跑                             
--  输 入 表：
--        ods.                                   -- 渠道表
--        ods.                                 	 -- 城市纬度表        
--        ods.                                   -- app包信息
--
--  输 出 表：julive_dim.dim_channel_info 
--
--  创 建 者：  未知  186xxx  xxx@julive.com
--  创建日期： xxx/08/07 14:16
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--  20210525  姜宝桥  修复重复数据，ods.yw_order_require where order_id in(54046,40168)

-- 计算线索楼盘业务列表 
drop table if exists tmp_dev_1.tmp_clue_project_type_name;
create table tmp_dev_1.tmp_clue_project_type_name as 

select 

tmp.clue_id,
concat_ws(',',collect_set(tmp.project_type_name)) as interest_project_type_name_list 

from (

select 

t1.order_id as clue_id,
if(t3.project_type = 1,"住宅",
if(t3.project_type = 2,"别墅",
if(t3.project_type = 3,"商住",
if(t3.project_type = 55,"商住",
if(t3.project_type = 58,"商住",
if(t3.project_type = 59,"商住",
null)))))) as project_type_name 

from (

select 

t.order_id,
project_name 

from ods.yw_order_require t 
lateral view explode(split(t.interest_project_name,',')) tmp as project_name 

) t1 
left join ods.yw_order t2 on t1.order_id = t2.id 
left join ods.cj_project t3 on t1.project_name = t3.name and t2.city_id = t3.city_id 

) tmp 
group by tmp.clue_id 
having concat_ws(',',collect_set(tmp.project_type_name)) != '' 
;


with tmp_clue_score as (
  SELECT order_id,
  sum(score) / count(order_id) AS clue_score -- 线索质量得分 20201218添加
  FROM ods.yw_cq_order
  GROUP BY order_id 
),
tmp_org_detail as (
   select 
     department_id,
      team_name
  from (select
      department_id,
      team_name,
      row_number() over (partition by department_id order by effective_date desc) as rn
      from ods.yw_department_architecture_history
      where pid =0 
  ) a 
  where rn = 1
--   select
--  department_id,
--  team_name
--  from ods.yw_department_architecture_history
--  where pid =0 
--  group by department_id,
--  team_name
)--公司名称20201224添加

-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------
-- 处理基础表数据 
insert overwrite table julive_dim.dim_clue_base_info 

select 

regexp_replace(uuid(),"-","")                                                            as skey,
t1.id                                                                                    as clue_id,
t1.org_id                                                                                as org_id,  --20201218添加
t1.org_type                                                                              as org_type,--20201218添加
t20.team_name                                                                            as org_name,--20201224添加
t1.channel_id                                                                            as channel_id,
t16.channel_name                                                                         as channel_name,
t1.city_id                                                                               as city_id, 
t2.city_name                                                                             as city_name,
t2.city_seq                                                                              as city_seq,
t1.source                                                                                as source,
t21.description                                                                          as source_desc, -- 用户来源
t1.user_id                                                                               as user_id,
t1.user_realname                                                                         as user_name,
t1.user_mobile                                                                           as user_mobile,
t1.creator                                                                               as creator, -- 20201015 
t1.sex                                                                                   as sex,
t1.employee_id                                                                           as emp_id,
t1.employee_realname                                                                     as emp_name,
t1.follow_service                                                                        as follow_service,
''                                                                                       as follow_service_name, -- 待梳理出员工维度后填充 
t1.status                                                                                as clue_status,
d4.tgt_code_name                                                                         as clue_status_tc,
t1.is_distribute                                                                         as is_distribute,
d2.tgt_code_name                                                                         as distribute_tc, -- 二级不分配原因
if(t6.order_id is not null,1,0)                                                          as unreasonable, -- 不合理关闭订单 
datediff(date_add(current_date(),-1),from_unixtime(t1.distribute_datetime,"yyyy-MM-dd")) as distribute_date_diff, -- 分配距今天数
t3.product_id                                                                            as product_id, 
''                                                                                       as product_name, -- 待处理 
t3.sub_product_id                                                                        as sub_product_id,
''                                                                                       as sub_product_name, -- 待处理 
t1.customer_intent_city                                                                  as customer_intent_city_id, -- 客户意向城市 20191009添加 
if(t9.city_name is not null,t9.city_name,t14.city_name)                                  as customer_intent_city_name, -- 客户意向城市 20191009添加 
t9.city_seq                                                                              as customer_intent_city_seq,

coalesce(t11.first_customer_intent_city_id,t1.customer_intent_city)                      as first_customer_intent_city_id, -- 20191118添加 
if(t9.city_name is not null,t9.city_name,t14.city_name)                                  as first_customer_intent_city_name,
t12.city_seq                                                                             as first_customer_intent_city_seq,

t1.op_type                                                                               as op_type,
t5.op_type_name_cn                                                                       as op_type_name_cn,
if(t1.source in (9,10),1,0)                                                              as is_400_called, -- 是否400来电,即客服订单 
case 
when t1.source in (9,10) then 'A'      -- A 正常订单 
when t3.rule_type = 3    then 'B'      -- B 缩短订单 
when t10.type = 1        then 'C'      -- C 参与试验缩短订单 
when t10.type = 2        then 'D'      -- D 参与试验正常订单 
else 'A' end                                                                             as short_order_tag,    -- 正常订单


null as is_recommend, -- 友介 
null as is_cust_recommend, -- 客户友介 
null as is_emp_recommend, -- 员工友介 
t1.is_short_alloc,
if(
if(t1.is_short_alloc = 1 and t1.short_alloc_type = 1,1,null) = 1 or 
if(t1.is_short_alloc = 1 and t1.short_alloc_type = 0,1,null) = 1,1
,0)                                                                                      as is_short_alloc_clue, -- 是否路径订单线索
-- 郭青路径缩短口径：is_short_alloc=1，short_alloc_type in（1 or 0）
-- 进伟：待整理
-- -- 最新口径
-- if(t1.short_alloc_type = 2,"客服分配", 
-- if(t1.short_alloc_type=1 or [is_short_alloc]=1 ,"线索直接分配",
-- if(t1.is_short_alloc!=1 and [跟进客服] is null AND [跟进客服]='' AND [is_distribute]=1,"手动添加",
--  if(t1.is_short_alloc!=1 and [跟进客服] is NOT null AND [跟进客服]!='' AND [is_distribute]=1,"客服分配",  
--    "未上户")))) as alloc_type,
-- 
-- -- 17年口径 
-- if([is_distribute]=27,"线索直接分配",if([source]=32,"线索直接分配",null))
t1.short_alloc_type,
t1.order_alloc_type,
t1.alloc_type,
if(t1.is_short_alloc = 1,"线索直接分配",
if(t1.is_short_alloc = 0 and t1.follow_service is null,"手动添加",
if(t1.follow_service = '' and t1.is_short_alloc = 0,"手动添加",
if(t1.is_short_alloc = 0 and t1.follow_service is not null,"客服分配",
if(t1.follow_service != '' and t1.is_short_alloc = 0,"客服分配",
"其他")))))                                                                              as first_distribute_source, -- 分配来源-初步 
t1.distribute_category,
d3.tgt_code_name                                                                         as distribute_category_desc, -- 一级不分配原因
if(t1.status = 0,"非正常结束(半路中断，或没买房)",
if(t1.status = 10,"未确认收到分配",
if(t1.status = 20,"已收到分配",
if(t1.status = 30,"已联系",
if(t1.status = 40,"已带看",
if(t1.status = 50,"已认购",
if(t1.status = 5,"未分配",
if(t1.status = 45,"排号",
if(t1.status = 55,"草签",
if(t1.status = 60,"已网签",
"其他"))))))))))                                                                         as sale_stage, -- 订单所处阶段 
if(t1.is_distribute = 27,"线索直接分配",
if(t1.source = 32,"线索直接分配",
"其他"))                                                                                 as is_first_shorten_path, -- 是否路径缩短计划-初步
if(
if(t1.is_distribute = 27,"线索直接分配",
if(t1.source = 32,"线索直接分配",
"其他")) = "其他",
if(t1.is_distribute = 27,"线索直接分配",
if(t1.source = 32,"线索直接分配",
"其他")),
if(t1.is_short_alloc = 1,"线索直接分配",
if(t1.is_short_alloc = 0 and t1.follow_service is null,"手动添加",
if(t1.follow_service = '' and t1.is_short_alloc = 0,"手动添加",
if(t1.is_short_alloc = 0 and t1.follow_service is not null,"客服分配",
if(t1.follow_service != '' and t1.is_short_alloc = 0,"客服分配",
"其他")))))
)                                                                                        as last_distribute_source, -- 分配来源-终 COALESCE([是否路径缩短计划-初步],[分配来源-初步])
if(t1.source = 5 
or t1.source = 6 
or t1.source = 9 
or t1.source = 10 
or t1.source = 14 
or t1.source = 19 
or t1.source = 28,"来电",
if(t1.source = 13 
or t1.source = 17 
or t1.source = 18 
or t1.source = 20,"友介",
if(t1.source = 12,"在线客服",
"留电")))                                                                                as cust_source_type, -- 来源归类 

from_unixtime(t1.distribute_datetime,'yyyy-MM-dd HH:mm:ss')                              as distribute_time,
from_unixtime(t1.distribute_datetime,'yyyy-MM-dd')                                       as distribute_date,
from_unixtime(t1.first_distribute_datetime,'yyyy-MM-dd HH:mm:ss')                        as first_distribute_time,
from_unixtime(t1.first_distribute_datetime,'yyyy-MM-dd')                                 as first_distribute_date,
from_unixtime(t1.confirm_distribute_datetime,'yyyy-MM-dd HH:mm:ss')                      as confirm_distribute_time,
from_unixtime(t1.confirm_distribute_datetime,'yyyy-MM-dd')                               as confirm_distribute_date,
from_unixtime(t1.re_distribute_datetime,'yyyy-MM-dd HH:mm:ss')                           as re_distribute_time,
from_unixtime(t1.re_distribute_datetime,'yyyy-MM-dd')                                    as re_distribute_date,
from_unixtime(t1.server_distribute_datetime,'yyyy-MM-dd HH:mm:ss')                       as server_distribute_time,
from_unixtime(t1.server_distribute_datetime,'yyyy-MM-dd')                                as server_distribute_date,
if(t1.re_distribute_datetime is null or t1.re_distribute_datetime = "",
from_unixtime(t1.confirm_distribute_datetime),
from_unixtime(t1.re_distribute_datetime))                                                as last_distribute_time, -- 最后一次分配时间:COALESCE([重新分配时间],[确认分配时间])
from_unixtime(t1.create_datetime,'yyyy-MM-dd HH:mm:ss')                                  as create_time,
from_unixtime(t1.create_datetime,'yyyy-MM-dd')                                           as create_date,
from_unixtime(t1.update_datetime,'yyyy-MM-dd HH:mm:ss')                                  as update_time,
from_unixtime(t1.update_datetime,'yyyy-MM-dd')                                           as update_date,

-- 订单需求表属性
t4.district_id                                                                           as district_id,
t4.district_other                                                                        as district_other,
t4.line                                                                                  as line,
t4.line_status                                                                           as line_status,
t4.line_max                                                                              as line_max,
t4.subway                                                                                as subway,
t4.address                                                                               as address,
t4.house_type                                                                            as house_type,
t4.project_type                                                                          as project_type,
if(t4.project_type=1,"住宅", 
if(t4.project_type=2,"别墅", 
if(t4.project_type=3, "商住",
if(t4.project_type=4, "其他", 
"未知"))))                                                                               as project_type_name, -- 业态名称
t4.acreage_min                                                                           as acreage_min,
t4.acreage_max                                                                           as acreage_max,
t4.total_price_min                                                                       as total_price_min,
t4.total_price_max                                                                       as total_price_max,
-- t8.grade_name                                                                         as budget_range_grade,
''                                                                                       as budget_range_grade,
t4.unit_price_min                                                                        as unit_price_min,
t4.unit_price_max                                                                        as unit_price_max,
t4.first_price_min                                                                       as first_price_min,
t4.first_price_max                                                                       as first_price_max,
t4.month_price_min                                                                       as month_price_min,
t4.month_price_max                                                                       as month_price_max,
t4.interest_project                                                                      as interest_project_id,
t4.recommend_project                                                                     as recommend_project_id,
t4.saw_project                                                                           as saw_project, 
from_unixtime(t4.see_datetime)                                                           as see_datetime,
t4.interest_project_name                                                                 as interest_project_name,
t7.interest_project_type_name_list                                                       as interest_project_type_name_list, -- 客户感兴趣楼盘业态列表-最新 
t4.recommend_project_name                                                                as recommend_project_name,
t4.saw_project_name                                                                      as saw_project_name,
t4.investment                                                                            as investment,
t4.qualifications                                                                        as qualifications,
t4.intent                                                                                as intent,
case
when t4.intent = 1 then "无意向"
when t4.intent = 2 then "保留"
when t4.intent = 3 then "有意向"
end                                                                                      as intent_tc,
from_unixtime(t4.intent_enddate)                                                         as intent_enddate,
from_unixtime(t4.intent_low_datetime)                                                    as intent_low_datetime,
t4.intent_employee_id                                                                    as intent_employee_id,
t4.intent_employee_name                                                                  as intent_employee_name,
t4.intent_no_like                                                                        as intent_no_like, 
from_unixtime(t4.lowintent_reincustomer_datetime)                                        as lowintent_reincustomer_datetime,
t4.intent_low_reason                                                                     as intent_low_reason,
t4.is_elite                                                                              as is_elite,
t4.building_type                                                                         as building_type,
t4.purchase_purpose                                                                      as purchase_purpose,
case 
when t4.purchase_purpose = 1 then "刚需"
when t4.purchase_purpose = 2 then "改善"
when t4.purchase_purpose = 3 then "投资"
end                                                                                      as purchase_purpose_tc,
case
when t4.purchase_urgency = 1 then "高"
when t4.purchase_urgency = 2 then "中"
when t4.purchase_urgency = 3 then "低"
end                                                                                      as purchase_urgency,
case
when t4.accept_other_area = 1 then "是"
when t4.accept_other_area = 2 then "否"
end                                                                                      as accept_other_area,
case
when t4.see_time = 1 then "工作日"
when t4.see_time = 2 then "周末"
end                                                                                      as see_time,
t4.see_time_desc                                                                         as see_time_desc,                                                               
if(t4.focus_point_new is not null and t4.focus_point_new != '',t4.focus_point_new,t4.focus_point) as focus_point,
t4.resistance_point                                                                      as resistance_point,
case
when t4.coordination_degree = 1 then "高"
when t4.coordination_degree = 2 then "中"
when t4.coordination_degree = 3 then "低"
end                                                                                      as coordination_degree,
t4.has_main_push_projects                                                                as has_main_push_projects,
t4.investors                                                                             as investors,
t4.decisionor                                                                            as decisionor,
t4.disturbor                                                                             as disturbor,
t4.family_funds                                                                          as family_funds,
t4.other_desc                                                                            as other_desc,
case 
when t4.estate_by_name = 0 then "无" 
when t4.estate_by_name = 1 then "有"
end as estate_by_name,
t4.estate_by_name_desc                                                                   as estate_by_name_desc,
case
when t4.whether_house = 1 then "否"
when t4.whether_house = 2 then "是"
end                                                                                      as whether_house,
t4.customer_description                                                                  as customer_description,
t4.close_explan                                                                          as close_explan,
t4.is_pause_follow                                                                       as is_pause_follow,
t19.clue_score                                                                           as clue_score,

if(t17.id is not null,1,0)                                                               as is_ai_service, -- 是否AI客服数据 20201015添加 
-- 多路判断用case when 
-- if(t13.id is null and t15.id is null and t17.id is null,1,
--    if(t13.id is not null,2,
--       if(t15.id is not null,3,
--         if(t17.id is not null,11,999))))                                                 as from_source, 
case 
when t13.id is not null then 2 -- 乌鲁木齐数据 
when t15.id is not null then 3 -- 二手房中介数据 
when t17.id is not null then 1 -- AI客服数据 
when (t1.org_type != 1 and t18.org_type != 1 and t18.join_type not in (0,1,2) and t18.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                                      as from_source, -- 业务线

case 
when t13.id is not null then 2 -- 乌鲁木齐数据 
when t15.id is not null then 3 -- 二手房中介数据 
when t17.id is not null then 11 -- AI客服数据 
when (t1.org_type != 1 and t18.org_type != 1 and t18.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                                      as from_source_detail, -- 数据来源 

map('default',null)                                                                      as separate_feature,

current_timestamp()                                                                      as etl_time 

-- ETL逻辑 -------------------------------------------------------------------------------------------------- 
from (

select * 
from ods.yw_order 
where is_distribute != 16 or is_distribute is null -- 剔除测试数据 不删除null值数据行 

) t1 
left join julive_dim.dim_city t2 on t1.city_id = t2.city_id -- 替换 
left join ods.yw_order_attr t3 on t1.id = t3.order_id 
left join (select *,row_number()over(partition by order_id order by update_datetime desc) rn from ods.yw_order_require) t4 on t1.id = t4.order_id and rn=1
left join julive_dim.dim_optype t5 on t1.op_type = t5.skey 
left join ( 

select order_id 
from ods.yw_order_business_tags 
where label_name= '不合理关订单'
group by order_id 

) t6 on t1.id = t6.order_id 
left join tmp_dev_1.tmp_clue_project_type_name t7 on t1.id = t7.clue_id 
-- left join julive_dim.dict_budget_range t8 on coalesce(t1.customer_intent_city,0) = t8.city_id -- 20201015 先暂停 
-- 转码 
left join julive_dim.dict_transcoding d1 on t1.source = d1.src_code_value and d1.code_type = 'source' and d1.from_tab_col = 'ods.yw_order.source' 
left join julive_dim.dict_transcoding d2 on t1.is_distribute = d2.src_code_value and d2.code_type = 'is_distribute' and d2.from_tab_col = 'ods.yw_order.is_distribute' 
left join julive_dim.dict_transcoding d3 on t1.distribute_category = d3.src_code_value and d3.code_type = 'distribute_category' and d3.from_tab_col = 'ods.yw_order.distribute_category' 
left join julive_dim.dict_transcoding d4 on t1.status = d4.src_code_value and d4.code_type = 'status' and d4.from_tab_col = 'ods.yw_order.status' 
left join julive_dim.dim_city t9 on t1.customer_intent_city = t9.city_id 
left join ( -- 20200205 发现订单有重复 临时修改 

select 

order_id,
-- type 
max(type) as type 

from ods.yw_path_short_alloc_log 
group by order_id 
-- type 

) t10 on t1.id = t10.order_id 
left join ( -- 底表已停止 ，属性暂时无用 ，待有意义时更新即可 

select 

t.order_id,
t.customer_intent_city as first_customer_intent_city_id 

from (
select 

order_id,
customer_intent_city,
row_number()over(partition by order_id order by create_datetime asc) as rn 

from ods.yw_order_history 
where customer_intent_city != 0 
) t  
where t.rn = 1 

) t11 on t1.id = t11.order_id 
left join julive_dim.dim_city t12 on coalesce(t11.first_customer_intent_city_id,t1.customer_intent_city) = t12.city_id 
left join ods.yw_developer_city_config t13 on t1.customer_intent_city = t13.city_id -- 乌鲁木齐 
left join julive_dim.dim_wlmq_city t14 on t1.customer_intent_city = t14.city_id 
left join ods.yw_esf_virtual_config t15 on t1.customer_intent_city = t15.virtual_city -- 二手房中介 
left join ods.cj_channel t16 on t16.channel_id = t1.channel_id 
left join ods.kefu_ai_alloc_customer_prepare t17 on t17.order_id = t1.id -- AI客服 
join ods.yw_org_info t18 on t1.org_id = t18.org_id and t1.org_type = t18.org_type -- 加盟商 ：20201015集成加盟商数据 
LEFT JOIN tmp_clue_score t19 ON t1.id = t19.order_id -- 线索质量得分 20201218添加
left join tmp_org_detail t20 on t1.org_id = t20.department_id --公司名称20201224添加
left join (
   select * from  ods.cj_source_config where business_line in(1,2)) t21 on t1.source=t21.source

-- where (coalesce(t4.total_price_max,0) >= t8.low_budget and coalesce(t4.total_price_max,0) < t8.high_budget) or t8.id is null
;



--二手房中介线索 xpt_order <-> xpt_order_attr from_source = 31 
-- insert into table julive_dim.dim_clue_base_info 
insert overwrite table julive_dim.dim_esf_clue_info 

select 

regexp_replace(uuid(),"-","")                                                            as skey,
t1.id                                                                                    as clue_id,
''                                                                                       as org_id,
''                                                                                       as org_type,
''                                                                                       as org_name,
t2.channel_id                                                                            as channel_id,
t3.channel_name                                                                          as channel_name,
t1.city_id                                                                               as city_id, 
t5.city_name                                                                             as city_name,
t5.city_seq                                                                              as city_seq,
t1.source                                                                                as source,
t6.description                                                                           as source_desc, -- 用户来源
t1.user_id                                                                               as user_id,
t1.user_realname                                                                         as user_name,
t1.user_mobile                                                                           as user_mobile,
t1.creator                                                                               as creator, -- 20201015 添加 
t1.sex                                                                                   as sex,
''                                                                                       as emp_id,
''                                                                                       as emp_name,
''                                                                                       as follow_service,
''                                                                                       as follow_service_name, -- 待梳理出员工维度后填充 
t1.status                                                                                as clue_status,
case
when t1.status =0 then '无效订单'  
when t1.status =1 then '有效状态'
when t1.status =10 then '待跟进'
when t1.status =20 then '跟进中'
when t1.status =30 then '已带看' 
when t1.status =40 then '已谈判'
when t1.status =50 then '已成交'
else '其他' end                                                                          as clue_status_tc,
t1.is_distribute                                                                         as is_distribute,
''                                                                                       as distribute_tc, -- 二级不分配原因
''                                                                                       as unreasonable, -- 不合理关闭订单 
datediff(date_add(current_date(),-1),from_unixtime(t1.distribute_datetime,"yyyy-MM-dd")) as distribute_date_diff, -- 分配距今天数
t2.product_id                                                                            as product_id, 
''                                                                                       as product_name, -- 待处理 
''                                                                                       as sub_product_id,
''                                                                                       as sub_product_name, -- 待处理 
''                                                                                       as customer_intent_city_id, -- 客户意向城市 20191009添加 
''                                                                                       as customer_intent_city_name, -- 客户意向城市 20191009添加 
''                                                                                       as customer_intent_city_seq,
''                                                                                       as first_customer_intent_city_id, -- 20191118添加 
''                                                                                       as first_customer_intent_city_name,
''                                                                                       as first_customer_intent_city_seq,
t2.op_type                                                                               as op_type,
t4.op_type_name_cn                                                                       as op_type_name_cn,
''                                                                                       as is_400_called, -- 是否400来电,即客服订单 
''                                                                                       as short_order_tag,    -- 正常订单
''                                                                                       as is_recommend, -- 友介 
''                                                                                       as is_cust_recommend, -- 客户友介 
''                                                                                       as is_emp_recommend, -- 员工友介 
''                                                                                       as is_short_alloc,
''                                                                                       as is_short_alloc_clue, -- 是否路径订单线索
''                                                                                       as short_alloc_type,
''                                                                                       as order_alloc_type,
''                                                                                       as alloc_type,
''                                                                                       as first_distribute_source, -- 分配来源-初步 
''                                                                                       as distribute_category,
''                                                                                       as distribute_category_desc, -- 一级不分配原因
''                                                                                       as sale_stage, -- 订单所处阶段 
''                                                                                       as is_first_shorten_path, -- 是否路径缩短计划-初步
''                                                                                       as last_distribute_source, -- 分配来源-终 COALESCE([是否路径缩短计划-初步],[分配来源-初步])
''                                                                                       as cust_source_type, -- 来源归类 
from_unixtime(t1.distribute_datetime,'yyyy-MM-dd HH:mm:ss')                              as distribute_time,
from_unixtime(t1.distribute_datetime,'yyyy-MM-dd')                                       as distribute_date,
''                                                                                       as first_distribute_time,
''                                                                                       as first_distribute_date,
''                                                                                       as confirm_distribute_time,
''                                                                                       as confirm_distribute_date,
''                                                                                       as re_distribute_time,
''                                                                                       as re_distribute_date,
''                                                                                       as server_distribute_time,
''                                                                                       as server_distribute_date,
''                                                                                       as last_distribute_time, -- 最后一次分配时间:COALESCE([重新分配时间],[确认分配时间])
from_unixtime(t1.create_datetime,'yyyy-MM-dd HH:mm:ss')                                  as create_time,
from_unixtime(t1.create_datetime,'yyyy-MM-dd')                                           as create_date,
from_unixtime(t1.update_datetime,'yyyy-MM-dd HH:mm:ss')                                  as update_time,
from_unixtime(t1.update_datetime,'yyyy-MM-dd')                                           as update_date,
''                                                                                       as district_id,
''                                                                                       as district_other,
''                                                                                       as line,
''                                                                                       as line_status,
''                                                                                       as line_max,
''                                                                                       as subway,
''                                                                                       as address,
''                                                                                       as house_type,
''                                                                                       as project_type,
''                                                                                       as project_type_name, -- 业态名称
''                                                                                       as acreage_min,
''                                                                                       as acreage_max,
''                                                                                       as total_price_min,
''                                                                                       as total_price_max,
''                                                                                       as budget_range_grade,
''                                                                                       as unit_price_min,
''                                                                                       as unit_price_max,
''                                                                                       as first_price_min,
''                                                                                       as first_price_max,
''                                                                                       as month_price_min,
''                                                                                       as month_price_max,
''                                                                                       as interest_project_id,
''                                                                                       as recommend_project_id,
''                                                                                       as saw_project, 
''                                                                                       as see_datetime,
''                                                                                       as interest_project_name,
''                                                                                       as interest_project_type_name_list, -- 客户感兴趣楼盘业态列表-最新 
''                                                                                       as recommend_project_name,
''                                                                                       as saw_project_name,
''                                                                                       as investment,
''                                                                                       as qualifications,
''                                                                                       as intent,
''                                                                                       as intent_tc,
''                                                                                       as intent_enddate,
''                                                                                       as intent_low_datetime,
''                                                                                       as intent_employee_id,
''                                                                                       as intent_employee_name,
''                                                                                       as intent_no_like, 
''                                                                                       as lowintent_reincustomer_datetime,
''                                                                                       as intent_low_reason,
''                                                                                       as is_elite,
''                                                                                       as building_type,
''                                                                                       as purchase_purpose,
''                                                                                       as purchase_purpose_tc,
''                                                                                       as purchase_urgency,
''                                                                                       as accept_other_area,
''                                                                                       as see_time,
''                                                                                       as see_time_desc,
''                                                                                       as focus_point,
''                                                                                       as resistance_point,
''                                                                                       as coordination_degree,
''                                                                                       as has_main_push_projects,
''                                                                                       as investors,
''                                                                                       as decisionor,
''                                                                                       as disturbor,
''                                                                                       as family_funds,
''                                                                                       as other_desc,
''                                                                                       as estate_by_name,
''                                                                                       as estate_by_name_desc,
''                                                                                       as whether_house,
''                                                                                       as customer_description,
''                                                                                       as close_explan,
''                                                                                       as is_pause_follow,
''                                                                                       as clue_score,
0                                                                                        as is_ai_service, -- 占位符 
3                                                                                        as from_source, -- 二手房中介
31                                                                                       as from_source_detail,--二手房中介新来源xpt_order
map('default',null)                                                                      as separate_feature,
current_timestamp()                                                                      as etl_time 

from ods.xpt_order t1 
join ods.xpt_order_attr t2 on t1.id = t2.order_id 
left join ods.cj_channel t3 on t3.channel_id = t2.channel_id 
left join julive_dim.dim_optype t4 on t2.op_type = t4.skey 
left join julive_dim.dim_wlmq_city t5 on t1.city_id = t5.city_id 
left join (
   select * from  ods.cj_source_config where business_line =4) t6 on t1.source=t6.source
;



-- ------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------
-- 加工子表 


-- 居理数据 
insert overwrite table julive_dim.dim_clue_info 
select t.* 
from julive_dim.dim_clue_base_info t 
where t.from_source = 1 
;
-- 乌鲁木齐数据 
insert overwrite table julive_dim.dim_wlmq_clue_info 
select t.* 
from julive_dim.dim_clue_base_info t 
where t.from_source = 2 
;
-- 二手房中介数据 
insert into table julive_dim.dim_esf_clue_info 
select t.* 
from julive_dim.dim_clue_base_info t 
where t.from_source = 3 
;
-- 加盟商数据 
insert overwrite table julive_dim.dim_jms_clue_info 
select t.* 
from julive_dim.dim_clue_base_info t 
where t.from_source = 4 
;
--内部加盟商
insert into table julive_dim.dim_jms_clue_info 
select t.*
from julive_dim.dim_clue_base_info t 
where t.from_source=1 and t.org_id !=48
;


