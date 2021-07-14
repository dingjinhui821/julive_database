set hive.execution.engine=spark;
set spark.app.name=dim_house_info;
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table julive_dim.dim_house_info 

select 
t1.house_type_id                 as house_id,              
t1.app_house_type_id,    
t1.house_type_num,       

t1.project_type, 
case 
when t1.project_type =1  then '住宅'
when t1.project_type =2  then '别墅'
when t1.project_type =3  then '商业'
when t1.project_type =55 then '商铺'
when t1.project_type =58 then '写字楼'
else '其它' 
end                             as project_type_desc,
t1.room_type, 
case 
when t1.room_type = 0 then '不限'
when t1.room_type = 1 then '一居'
when t1.room_type = 2 then '二居'
when t1.room_type = 3 then '三居'
when t1.room_type = 4 then '四居'
when t1.room_type = 5 then '五居及以上'
when t1.room_type = 6 then 'loft'
when t1.room_type = 7 then '开间'
else '其它' 
end                             as room_type_desc,        
t1.summary,               
t1.is_show_room_type,     
t1.orientation,           
t1.good_desc,             
t1.bad_desc,              
t1.is_loft,              
t1.building_type, 
case 
when t1.building_type = 1 then '独栋'
when t1.building_type = 2 then '双拼'
when t1.building_type = 3 then '联排'
when t1.building_type = 4 then '叠加'
when t1.building_type = 5 then '平层'
when t1.building_type = 6 then '复式'
when t1.building_type = 7 then '洋房'
when t1.building_type = 8 then '高层顶复'
when t1.building_type = 9 then '高层底复'
when t1.building_type = 10 then '洋房顶复'
when t1.building_type = 11 then '洋房底复'
when t1.building_type = 12 then '类独栋'
when t1.building_type = 13 then '合院'
when t1.building_type = 14 then '复式'
when t1.building_type = 15 then '平层'

else '其它' 
end                           as building_type_desc,    
t1.house_tag,             
t1.small_family,          
t1.low_price,             
t1.layout_shi,           
t1.layout_ting,           
t1.layout_chu,            
t1.layout_wei,            
t1.master_bed_room,       
t1.toilet,                
t1.living_room,           
t1.kitchen,               
t1.restaurant,            
t1.terrace,               

t1.acreage,               
t1.ac_acreage,            
t1.price,                 
t1.down_pay,              
t1.payment_ratio,         
t1.month_pay,             
t1.total_count,           
t1.surplus_count,         
t1.trans_price,          
t1.offer_price,           
t1.rate,                  

t1.status, 
case 
when t1.status = 1 then '未售'
when t1.status = 2 then '在售'
when t1.status = 3 then '售罄'
when t1.status = 4 then '待售'
else '其它'
end                             as status_desc,        
           
t1.house_on_sale_num,     
t1.house_on_sale_tag, 
case 
when t1.house_on_sale_tag = 1 then '房源充足'
when t1.house_on_sale_tag = 2 then '仅剩顶底层'
when t1.house_on_sale_tag = 3 then '仅剩顶层'
when t1.house_on_sale_tag = 4 then '仅剩底层'
when t1.house_on_sale_tag = 5 then '少于10套'
else '其它'                 
end                             as house_on_sale_tag_desc,
t1.is_false,              
t1.project_id,           
t1.employee_id,           
t1.employee_name,         
t1.create_datetime,       
t1.update_datetime

from ods.cj_house_type t1  
;

