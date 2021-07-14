-------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------
-- ETL ------------------------------------------------------------------------------------------------------------------------ 
set hive.mapred.supports.subdirectories=true;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set mapred.job.name=fact_grass_sign_dtl;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
with tmp_project_map_info1 as (

select 

t.project_id               as project_id,
t.city_id                  as city_id,
t.name                     as name,
t.address                  as address,
t.tag                      as tag,
t.api_type                 as api_type,
t.area                     as area,
t.lng                      as lng,
t.lat                      as lat,
t.project_lng              as project_lng,
t.project_lat              as project_lat,
t.distance                 as linear_distance,
t.walk_distance            as walk_distance,
t.walk_duration            as walk_duration, -- 单位分钟 
t.is_com_walk              as is_com_walk, 

-- 转码属性,清洗一级编码 
case
when t.name regexp('.*停车场.*') then  1 
when t.name regexp('.*幼儿园.*') then  2 
when t.name regexp('.*小学|附属小学|小学部|一小|二小|附小.*') then 2
when t.name regexp('.*小学|中学|一小|二小|一中|二中|三中|附中|初中.*') then 2
when t.name regexp('.*医学院$\|研究所$\|学院办公楼$\|专业学校$\|大学$\|学院$') and t.julive_type_one != 2 then 2
when regexp_extract(t.name,'(.*?)大学(.*?)附属(.*?)医院(.*?)',0) != '' then 4
when regexp_extract(t.name,'(.*?)大学(.*?)图书馆(.*?)',0) != '' then 2
when regexp_extract(t.name,'(.*?)大学(.*?)号楼(.*?)',0) != '' then 2
when regexp_extract(t.name,'(.*?)大学(.*?)幼儿(.*?)',0) != '' then 2
when regexp_extract(t.name,'(.*?)学校(.*?)分校(.*?)',0) != '' then 2
when t.name regexp('.*高尔夫|烤肉|健身|跆拳道|星巴克|coffee|咖啡|CAFE|Coffee|COFEE|李先生|烤肉|家常菜|水饺|小馆|煎饼|南城香|羊肉馆|5号坊|火烧|酒坊|大排档|农家乐|酒馆|翅吧|沙拉|面食|农家|兰芳宴|蛋糕|味多美|滋味居|拌饭|菜馆|太和农圃|农家院|福祥林|小吃|蒸饺|饭店|食堂|饭店|拉面|餐厅|肉饼店|铜锅涮|李先生|饺子馆|饺子|锅贴|驴肉馆|粗粮王|香饺|旺饺.*') then 3
when t.name regexp('.*医院$\|诊所$\|卫生室$\|药店$\|药房$\|卫生服务中心$\|医务室$\|口腔$\|卫生服务站$') then 4
when regexp_extract(t.name,'(.*?)附属(.*?)医院(.*?)',0) != '' then 4
when regexp_extract(t.name,'(.*?)大学(.*?)医院(.*?)',0) != '' then 4
when regexp_extract(t.name,'(.*?)医院(.*?)院区(.*?)',0) != '' then 4
else t.julive_type_one 
end                        as julive_type_one_code,
t.julive_type_two          as julive_type_two_code,

t.map_type_one             as map_type_one_code,
t.map_type_two             as map_type_two_code 

from ods.cj_project_map_info t 

where t.lat is not null 
and t.lng is not null
and t.lat != '' 
and t.lng != '' 
and t.area is not null   ---- 同一个poi点都存在区域不是空的  所以可以直接把区域是空的直接过滤掉
and t.area != '' 
and t.area != '区' 
and t.project_id != 333333333 

),
tmp_project_map_info2 as (

select 

t.project_id,
t.city_id,
t.name,
t.address,
t.tag,
t.api_type,
t.area,
t.lng,
t.lat,
t.project_lng,
t.project_lat,
t.linear_distance,
t.walk_distance,
t.walk_duration,
t.is_com_walk,

-- 转码属性,清洗二级编码 
t.julive_type_one_code,
case
when t.name regexp('.*停车场.*') and t.julive_type_one_code = 1 then 3
when t.name regexp('.*幼儿园.*') and t.julive_type_one_code = 2 then 4
when regexp_extract(t.name,'(.*?)大学(.*?)幼儿(.*?)',0) != '' then 4
when t.name regexp('.*小学|附属小学|小学部|一小|二小|附小.*') and t.julive_type_one_code = 2 then 5
when t.name regexp('.*中学|附属中学|一中|二中|三中|附中|初中.*') and t.julive_type_one_code = 2 then 6 
when regexp_extract(t.name,'(.*?)学校(.*?)分校(.*?)',0) != '' then 6 
when t.name regexp('.*附属.*') and t.julive_type_one_code = 2 and t.julive_type_two_code = 14 then 6  ----  如果名称里带附属  又是学校  被标记成大学的统一标记成中学
when t.name regexp('.*超市|商厦|商场|百货|商业中心|商城.*') and t.julive_type_one_code = 3 then 7
when t.name regexp('.*星巴克|coffee|咖啡|CAFE|Coffee|COFEE|李先生|烤肉|家常菜|水饺|小馆|煎饼|南城香|羊肉馆|5号坊|火烧|酒坊|大排档|农家乐|酒馆|翅吧|沙拉|面食|农家|兰芳宴|蛋糕|味多美|滋味居|拌饭|菜馆|太和农圃|农家院|福祥林|小吃|蒸饺|饭店|食堂|饭店|拉面|餐厅|肉饼店|铜锅涮|李先生|饺子馆|饺子|锅贴|驴肉馆|粗粮王|香饺|旺饺.*') and t.julive_type_one_code = 3 then 8
when t.name regexp('.*高尔夫|健身|跆拳道|舞蹈|马术|户外运动.*') and t.julive_type_one_code = 3 then 10
when t.name regexp('.*公园.*') and t.julive_type_one_code = 3 then 11 
when t.name regexp('.*影城|影院.*') and t.julive_type_one_code = 3 then 11
when t.name regexp('.*医院$\|诊所$\|卫生室$\|卫生服务中心$\|医务室$\|卫生服务站$\|口腔$') and t.julive_type_one_code = 4 then 12 
when regexp_extract(t.name,'(.*?)附属(.*?)医院(.*?)',0) != '' then 12
when regexp_extract(t.name,'(.*?)大学(.*?)医院(.*?)',0) != '' then 12
when regexp_extract(t.name,'(.*?)医院(.*?)院区(.*?)',0) != '' then 12
when t.name regexp('.*药店$\|药房$') then 13
when t.name regexp('.*医学院$\|研究所$\|学院办公楼$\|专业学校$\|大学$\|学院$') and t.julive_type_one_code = 2 then 14 
when regexp_extract(t.name,'(.*?)大学(.*?)图书馆(.*?)',0) != '' then 14
when regexp_extract(t.name,'(.*?)大学(.*?)号楼(.*?)',0) != '' then 14
else julive_type_two_code 
end as julive_type_two_code,

t.map_type_one_code,
t.map_type_two_code 

from tmp_project_map_info1 t 

),
tmp_project_map_info3 as (

select 

t.project_id,
t.city_id,
t.name,
t.address,
t.tag,
t.api_type,
t.area,
t.lng,
t.lat,
t.project_lng,
t.project_lat,

t.julive_type_one_code,
t.julive_type_two_code, 

t.map_type_one_code,
t.map_type_two_code,
t.is_com_walk,

max(t.linear_distance) as linear_distance,
max(t.walk_distance) as walk_distance,
max(t.walk_duration) as walk_duration 


from tmp_project_map_info2 t 
group by 

t.project_id,
t.city_id,
t.name,
t.address,
t.tag,
t.api_type,
t.area,
t.lng,
t.lat,
t.project_lng,
t.project_lat,
t.is_com_walk,

t.julive_type_one_code,
t.julive_type_two_code, 
t.map_type_one_code,
t.map_type_two_code

)


insert overwrite table julive_fact.fact_project_map_info 

select 

t1.project_id,
t2.project_name,
t1.city_id,
t3.city_name,

t1.name,
t1.address,
t1.tag,
t1.api_type,
t1.area,
t1.lng,
t1.lat,
t1.project_lng,
t1.project_lat,
t1.linear_distance,
t1.walk_distance,
t1.walk_duration,
t1.is_com_walk,

-- 转码属性 
t1.julive_type_one_code,
coalesce(t4.type_one_name,'未知') as julive_type_one_name,
t1.julive_type_two_code,
coalesce(t4.type_two_name,'未知') as julive_type_two_name,

t1.map_type_one_code,
coalesce(t5.type_one_name,'未知') as map_type_one_name,
t1.map_type_two_code,
coalesce(t5.type_two_name,'未知') as map_type_two_name,

current_timestamp() as etl_time 


from tmp_project_map_info3 t1 
left join julive_dim.dim_project_info t2 on t1.project_id = t2.project_id and t2.end_date = '9999-12-31' 
left join julive_dim.dim_city t3 on t1.city_id = t3.city_id 

left join julive_dim.dict_project_map_code t4 on t1.julive_type_two_code = t4.type_two_code and t4.code_type = 1 -- 居理 
left join julive_dim.dict_project_map_code t5 on t1.map_type_two_code = t5.type_two_code and t5.code_type = 2 -- 百度 
;


