--
-- /data1/etl/streaming/spark-2.4.0-bin-hadoop2.6/bin/spark-submit \
-- --name mysql_hive_kafka \
-- --class com.julive.api.BatchDataSend \
-- --master yarn-client \
-- --driver-cores 1 \
-- --driver-memory 2G \
-- --num-executors 2 \
-- --executor-cores 1 \
-- --executor-memory 2G \
-- --queue root.etl \
-- --jars /data4/nfsdata/julive_dw/spark/jdp-batchdata-send-0.0.1-SNAPSHOT.jar \
-- /data4/nfsdata/julive_dw/spark/jdp-batchdata-send-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
-- '{"url":"jdbc:mysql://192.168.10.18:3306/julive_dw","username":"root","password":"O@DUXwg^PebmSHY!"}' 12
--

-- 产品 ： 苑福 朱亚楠
-- -----------------------------------------------------------------------------
-- 楼盘列表-快捷筛选区域 -------------------------------------------------------
-- -----------------------------------------------------------------------------
-- type : project_list_quick_screening

-- SQL文件位置：/data4/nfsdata/julive_dw/etl/sql/julive_app/tmp_project_top_list_for_yf.sql

-- 过期口径 20210113 ，取数据逻辑参考21
-- drop table if exists tmp_dev_1.tmp_project_top_list_for_yf;
-- create table tmp_dev_1.tmp_project_top_list_for_yf stored as parquet as

-- select

-- select_city               as city_id,
-- district                  as district,
-- global_id                 as global_id


-- from julive_fact.fact_event_dtl t1
-- lateral view explode(split(regexp_replace(substr(get_json_object(properties,'$.district'),2,length(get_json_object(properties,'$.district'))-2),'"',''),',')) tbl as district

-- where t1.pdate >= regexp_replace(date_add(current_date(),-7),'-','')
-- and t1.pdate <= regexp_replace(date_add(current_date(),-1),'-','')
-- and event = 'e_filter_project'
-- and frompage in ('p_project_list','p_project_search_result_list','p_home')
-- and substr(product_id,1,3) in ('101','201','301')

-- and select_city is not null
-- and select_city != ''
-- and get_json_object(properties,'$.district') is not null
-- and get_json_object(properties,'$.district') <> ''
-- and get_json_object(properties,'$.district') <> '-1'

-- group by select_city,district,global_id
-- ;


-- -- 发送kafka  结果 SQL代码
-- drop table if exists tmp_dev_1.tmp_project_top_list_for_yf_result;
-- create table tmp_dev_1.tmp_project_top_list_for_yf_result as

-- with tmp_base as (

-- select

-- t1.city_id,
-- coalesce(t2.city_uv,0) as city_uv,

-- t1.district,
-- coalesce(t1.distinct_uv,0) as distinct_uv,
-- round(t1.distinct_uv / t2.city_uv,4) as active_rate

-- from (

-- select

-- city_id,
-- district,
-- count(distinct global_id) as distinct_uv

-- from tmp_dev_1.tmp_project_top_list_for_yf
-- group by city_id,district

-- ) t1 join (

-- select

-- city_id,
-- count(distinct global_id) as city_uv

-- from tmp_dev_1.tmp_project_top_list_for_yf
-- group by city_id

-- ) t2 on t1.city_id = t2.city_id

-- ),tmp_result as (

-- select

-- city_id,
-- city_uv,

-- district,
-- distinct_uv,
-- round(active_rate,4)*10000 as active_rate,

-- row_number()over(partition by city_id order by active_rate desc) as rn

-- from tmp_base

-- )

-- select

-- city_id as city_id,
-- city_uv as uv,

-- collect_set(concat("{'district':'",  coalesce(district,0),  "',",  "'distinct_uv':'",coalesce(distinct_uv,0),"',","'percentage':'",coalesce(concat(cast(active_rate as decimal)/100,'%'),0),"'}")) as district
-- -- collect_set(str_to_map(concat('district',':',district,'&','distinct_uv',':',distinct_uv,'&','percentage',':',concat(cast(active_rate as decimal)/100,'%')),'&',':')) as district

-- from tmp_result
-- where rn <= 10

-- group by city_id ,city_uv
-- ;


-- 新逻辑
drop table if exists tmp_dev_1.tmp_project_top_list_for_yf_result;
create table tmp_dev_1.tmp_project_top_list_for_yf_result as

with tmp_district as ( -- 区域数据

select

t3.city_id,
t2.district_id,
sum(t1.see_num) as see_num,
row_number()over(partition by t3.city_id order by sum(t1.see_num) desc,t2.district_id asc) as rn

from julive_fact.fact_see_project_dtl t1
join julive_dim.dim_project_info t2 on t1.project_id = t2.project_id
join julive_dim.dim_district_info t3 on t2.district_id = t3.district_id 

where to_date(t1.plan_real_begin_time) >= date_add(current_date(),-7) -- 取当前城市近7天
and to_date(t1.plan_real_begin_time) <= date_add(current_date(),-1)

group by t3.city_id,t2.district_id -- 楼盘对应区域

),
tmp_city as ( -- 城市数据

select

t3.city_id,
sum(t1.see_num) as see_num

from julive_fact.fact_see_project_dtl t1
join julive_dim.dim_project_info t2 on t1.project_id = t2.project_id
join julive_dim.dim_district_info t3 on t2.district_id = t3.district_id 

where to_date(t1.plan_real_begin_time) >= date_add(current_date(),-7) -- 取当前城市近7天
and to_date(t1.plan_real_begin_time) <= date_add(current_date(),-1)

group by t3.city_id -- 楼盘对应区域

),
tmp_result as ( -- 区域/城市
select

t1.city_id                                       as city_id,
-- t1.district_id                                   as district,
t3.name_pinyin                                   as district,
t1.see_num                                       as district_uv,
t2.see_num                                       as city_uv,
round(t1.see_num/t2.see_num,4)*10000             as active_rate

from tmp_district t1
join tmp_city t2 on t1.city_id = t2.city_id
left join ods.cj_district t3 on t1.district_id = t3.id

where t1.rn <= 6
)

select

city_id as city_id,
city_uv as uv,
collect_set(concat("{'district':'",  coalesce(district,0),  "',",  "'distinct_uv':'",coalesce(district_uv,0),"',","'percentage':'",coalesce(concat(cast(active_rate as decimal)/100,'%'),
'0%'),"'}")) as district

from tmp_result
group by city_id ,city_uv
;

-- ------------------------------------------------------------------------------------------------------
-- 发送kafka
-- select
--
-- city_id,
-- coalesce(uv,0) as uv,
-- concat('[',concat_ws(',',district),']') as district
--
-- from tmp_dev_1.tmp_project_top_list_for_yf_result
-- ;
--


-- ------------------------------------------------------------------------------------------------------
-- 透传需求 21 -------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------

-- 需求地址 ：http://cwiki.comjia.com/pages/viewpage.action?pageId=716963987
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- 区域带看倒排需求：
-- 数据结构：
-- {
--   "type": "project_list_quick_screening_for_district", -- 需求唯一标识ID
--   "data": [ -- 封装结果数据
--     {
--       "city_id": "20000257", -- 城市ID
--       "uv": "298", -- 城市带看量
--       "district": "[{'district':'jiangyin','distinct_uv':'101','percentage':'33.89%'},{'district':'xinwu','distinct_uv':'70','percentage':'23.49%'}]" -- 区域数据
--     }
--   ]
-- }
-- data.district属性子字符串含义
-- {
--    'district':'jiangyin', -- 区域ID
--    'distinct_uv':'101', -- 区域带看量
--    'percentage':'33.89%' -- 当前区域的带看量/城市全部区域的带看量 ，结果百分比
-- }


drop table if exists tmp_dev_1.tmp_city_district_top_seenum;
create table tmp_dev_1.tmp_city_district_top_seenum as

with tmp_district as ( -- 区域数据

select

t3.city_id,
t2.district_id,
sum(t1.see_num) as see_num,
row_number()over(partition by t3.city_id order by sum(t1.see_num) desc,t2.district_id asc) as rn

from julive_fact.fact_see_project_dtl t1
join julive_dim.dim_project_info t2 on t1.project_id = t2.project_id
join julive_dim.dim_district_info t3 on t2.district_id = t3.district_id

where to_date(t1.plan_real_begin_time) >= date_add(current_date(),-7) -- 取当前城市近7天
and to_date(t1.plan_real_begin_time) <= date_add(current_date(),-1)

group by t3.city_id,t2.district_id -- 楼盘对应区域

),
tmp_city as ( -- 城市数据

select

t3.city_id,
sum(t1.see_num) as see_num

from julive_fact.fact_see_project_dtl t1
join julive_dim.dim_project_info t2 on t1.project_id = t2.project_id
join julive_dim.dim_district_info t3 on t2.district_id = t3.district_id

where to_date(t1.plan_real_begin_time) >= date_add(current_date(),-7) -- 取当前城市近7天
and to_date(t1.plan_real_begin_time) <= date_add(current_date(),-1)

group by t3.city_id -- 楼盘对应区域

),
tmp_result as ( -- 区域/城市
select

t1.city_id                                       as city_id,
t1.district_id                                   as district,
t1.see_num                                       as district_uv,
t2.see_num                                       as city_uv,
round(t1.see_num/t2.see_num,4)*10000             as active_rate

from tmp_district t1
join tmp_city t2 on t1.city_id = t2.city_id

where t1.rn <= 6
)

select

city_id as city_id,
city_uv as uv,
collect_set(concat("{'district':'",  coalesce(district,0),  "',",  "'distinct_uv':'",coalesce(district_uv,0),"',","'percentage':'",coalesce(concat(cast(active_rate as decimal)/100,'%'),
'0%'),"'}")) as district

from tmp_result
group by city_id ,city_uv
;


-- 发送kafka
-- app
-- select
--
-- city_id,
-- coalesce(uv,0) as uv,
-- concat('[',concat_ws(',',district),']') as district
--
-- from tmp_dev_1.tmp_city_district_top_seenum
-- ;



-- ---------------------------------------------------------------------------
-- 透传需求 22 ----------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- 特色需求 ：
-- {
--   "type": "project_list_quick_screening_for_features", -- 需求唯一标识ID
--   "data": [ -- 封装结果数据
--     {
--       "city_id": "20000257", -- 城市ID
--       "uv": "298", -- 特色uv
--       "features": "[{'features':'h001','features_uv':'101','percentage':'33.89%'},{'features':'h002','features_uv':'70','percentage':'23.49%'}]" -- 特色数据
--     }
--   ]
-- }
-- data.features属性子字符串含义
-- {
--    'features':'jiangyin', -- 特色ID
--    'features_uv':'101', -- 特色UV
--    'percentage':'33.89%' -- features_uv/city_uv ，结果百分比
-- }


drop table if exists tmp_dev_1.tmp_project_top_list_for_features;
create table tmp_dev_1.tmp_project_top_list_for_features stored as parquet as

select

select_city               as city_id,
features                  as features, -- 特色
global_id                 as global_id

from julive_fact.fact_event_dtl t1
lateral view explode(split(regexp_replace(substr(get_json_object(properties,'$.features'),2,length(get_json_object(properties,'$.features'))-2),'"',''),',')) tbl as features

where t1.pdate >= regexp_replace(date_add(current_date(),-7),'-','')
and t1.pdate <= regexp_replace(date_add(current_date(),-1),'-','')

and event = 'e_filter_project'
and frompage in ('p_project_list','p_project_search_result_list')
and substr(product_id,1,3) in ('101','201')

and select_city is not null
and select_city != ''
and get_json_object(properties,'$.features') is not null
and get_json_object(properties,'$.features') <> ''
and get_json_object(properties,'$.features') <> '-1'

group by select_city,features,global_id
;


drop table if exists tmp_dev_1.tmp_project_top_list_for_features_result;
create table tmp_dev_1.tmp_project_top_list_for_features_result as

with tmp_features as ( -- 城市 特殊

select

city_id,
features,
count(distinct global_id) as features_uv

from tmp_dev_1.tmp_project_top_list_for_features
group by city_id,features

),
tmp_city as ( -- 城市

select

city_id,
count(distinct global_id) as city_uv

from tmp_dev_1.tmp_project_top_list_for_features
group by city_id

),
tmp_result as (

select

t1.city_id,
t1.features,
t1.features_uv,
t2.city_uv,
round(t1.features_uv/t2.city_uv,4)*10000 as active_rate,

row_number()over(partition by t1.city_id order by t1.features_uv desc) as rn

from tmp_features t1
join tmp_city t2 on t1.city_id = t2.city_id

)

select

city_id as city_id,
city_uv as uv,
collect_set(concat("{'features':'",  coalesce(features,0),  "',",  "'features_uv':'",coalesce(features_uv,0),"',","'percentage':'",coalesce(concat(cast(active_rate as decimal)/100,'%'),
'0%'),"'}")) as features

from tmp_result
where rn <= 8
group by city_id ,city_uv
;


-- 发送kafka
-- select
--
-- city_id,
-- coalesce(uv,0) as uv,
-- concat('[',concat_ws(',',features),']') as features
--
-- from tmp_dev_1.tmp_project_top_list_for_features_result
-- ;
