set etl_date = '${hiveconf:etldate}';
set hive.execution.engine=spark;
set spark.app.name=djh;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists test.tmp_employee_base;
create table test.tmp_employee_base as 

select 

t1.employee_id                                               as emp_id,
t1.employee_name                                             as emp_name,
t1.job_number                                                as job_number,
t1.idcard                                                    as card_no,
t1.sex                                                       as sex,
t1.nation                                                    as nation, -- 民族 
t1.domicile_address                                          as domicile_address, -- 户籍
t1.marital_status                                            as marital_status, -- 婚姻状况
to_date(from_unixtime(t1.birth))                             as birthday, 
t1.age                                                       as age,
t1.employment_form                                           as employment_form, -- 聘用形式
t1.political_outlook                                         as political_outlook, -- 政治面貌
t1.graduation_school                                         as graduation_school, -- 毕业学校
t1.school_attributes                                         as school_attributes, -- 学校等级 
to_date(from_unixtime(t1.graduation_time))                   as graduation_date, -- 毕业日期 
t1.major                                                     as major, -- 专业 
to_date(from_unixtime(t1.first_worktime))                    as first_work_date,
t1.high_major                                                as high_major,
t1.contract                                                  as contract,
t1.ascription                                                as ascription, -- 社保、公积金归属
to_date(from_unixtime(t1.entry_datetime))                    as entry_date, -- 入职日期, 计算 
to_date(from_unixtime(t1.full_time))                         as full_date, -- 转正日期, 计算
t1.full_type                                                 as full_type,
to_date(from_unixtime(t1.offjob_time))                       as offjob_date, -- 离职日期, 计算 
to_date(from_unixtime(t1.first_contract_time))               as first_contract_date,
to_date(from_unixtime(t1.end_contract_time))                 as end_contract_date,
t1.post_id                                                   as post_id,
t1.post_name                                                 as post_name,
t1.management_form                                           as management_form,
t1.department_attributes                                     as dept_attr,
t1.city_id                                                   as city_id,
t1.status                                                    as job_status, -- 员工状态, 计算
t1.direct_leader_id                                          as direct_leader_id,
t1.leader_employee_id                                        as leader_emp_id, -- 20191104添加 
t1.leader_job_number                                         as leader_job_number, -- 20191104添加 
t1.adjust_city_id                                            as adjust_city_id, -- 20191104添加 
t1.department_id                                             as dept_id,
t1.department_name                                           as dept_name,
to_date(from_unixtime(t1.create_datetime))                   as create_date,
max(to_date(from_unixtime(t1.check_date)))                   as check_date  -- 20191104添加 

from (select table3.* from (
select table1.*
from 
 ods.hr_manpower_monthly table1 
where  table1.city_id not in(select city_id from ods.yw_developer_city_config table2 ))table3) t1 
where t1.employee_id is not null 
and t1.employee_id != 0 
and t1.employee_id != -1 
and to_date(from_unixtime(t1.create_datetime))<=${hiveconf:etl_date}
and t1.create_datetime >=999999999
group by t1.employee_id,
t1.employee_name,
t1.job_number,                                           
t1.idcard,
t1.sex,
t1.nation,
t1.domicile_address,
t1.marital_status,
t1.birth,                        
t1.age,
t1.employment_form,                                      
t1.political_outlook,                                    
t1.graduation_school,                                    
t1.school_attributes,                                    
t1.graduation_time,              
t1.major,                                                
t1.first_worktime,               
t1.high_major,                                           
t1.contract,                                             
t1.ascription,                                           
t1.entry_datetime,               
t1.full_time,                   
t1.full_type,                                            
t1.offjob_time,              
t1.first_contract_time,
t1.end_contract_time,
t1.post_id,                                              
t1.post_name,                                          
t1.management_form,                                      
t1.department_attributes,                               
t1.city_id,                                              
t1.status,                                               
t1.direct_leader_id,                                     
t1.leader_employee_id,                                   
t1.leader_job_number,                                    
t1.adjust_city_id,                                       
t1.department_id,                                        
t1.department_name,                                      
t1.create_datetime
;


drop table if exists tmp_dev_1.tmp_employee_base_info_test;
create table tmp_dev_1.tmp_employee_base_info_test as

select 

emp_id,
emp_name,
job_number,
card_no,
sex,
nation,
domicile_address,
marital_status,
birthday,
age,
employment_form, 
political_outlook,
graduation_school,
school_attributes,
graduation_date,
major,
first_work_date,
high_major,
contract,
ascription,
entry_date,
entry_date_1,
full_date,
full_date_1,
full_type,
offjob_date,
offjob_date_1,
first_contract_date,
end_contract_date,
post_id,
post_name,
management_form,
dept_attr,
city_id,
job_status,
job_status_1,
direct_leader_id,
leader_emp_id, -- 20191104添加 
leader_job_number, -- 20191104添加 
adjust_city_id, -- 20191104添加 
dept_id,
dept_name,
create_date,
check_date, -- 20191104添加 

${hiveconf:etl_date} as p_date 

from (

select 

t1.emp_id,
t1.emp_name,
t1.job_number,
t1.card_no,
t1.sex,
t1.nation,
t1.domicile_address,
t1.marital_status,
t1.birthday,
t1.age,
t1.employment_form,
t1.political_outlook,
t1.graduation_school,
t1.school_attributes,
t1.graduation_date,
t1.major,
t1.first_work_date,
t1.high_major,
t1.contract,
t1.ascription,
t1.full_type,
t1.first_contract_date,
t1.end_contract_date,
t1.post_id,
t1.post_name,
t1.management_form,
t1.dept_attr,
t1.direct_leader_id,
t1.dept_id,
t1.dept_name,
t1.create_date,
t1.leader_emp_id, -- 20191104添加 
t1.leader_job_number, -- 20191104添加 
t1.check_date, -- 20191104添加 

if(t1.city_id in (0,-1),t1.adjust_city_id,t1.city_id)        as city_id, -- 20191104添加 
if(t1.adjust_city_id in (0,-1),t1.city_id,t1.adjust_city_id) as adjust_city_id, -- 20191104添加 

t1.entry_date, -- 计算 ,没毛病 
min(t1.create_date)over(partition by t1.emp_id) as entry_date_1,

t1.full_date, -- 计算 ，后续需要对 9999-12-31 进行处理 
min(if(t1.full_type = 2,t1.create_date,'9999-99-99'))over(partition by t1.emp_id) as full_date_1,


-- 计算属性 
-- 删除场景：操作员工离职，会将离职日期之后的数据删掉。
t1.job_status, -- 计算 , 没毛病 
if(t1.create_date = ${hiveconf:etl_date},1,0) as job_status_1,

if(t1.create_date is not null and t1.create_date<${hiveconf:etl_date},t1.create_date,t1.offjob_date) as offjob_date, -- 计算 ,离职人计算结果正确，在职需要根据job_status处理
max(if(t1.create_date = ${hiveconf:etl_date},'9999-99-99',t1.create_date))over(partition by t1.emp_id) as offjob_date_1,


row_number()over(partition by t1.emp_id order by t1.create_date desc) as rn 

from test.tmp_employee_base t1 
where t1.create_date <= ${hiveconf:etl_date} 

) t 
where t.rn = 1 
;




insert overwrite table tmp_dev_1.tmp_employee_current_data partition(p_date =${hiveconf:etl_date}) 

select 

regexp_replace(uuid(),'-','')             as skey,

t1.emp_id,
t1.emp_name,
t1.job_number,
t1.card_no ,

t1.sex,
case 
when t1.sex = 1 then "男" 
when t1.sex = 2 then "女"
else "未知" end                            as sex_tc,

t1.nation,
t1.domicile_address,

t1.marital_status,
case 
when t1.marital_status = 1 then "已婚" 
when t1.marital_status = 2 then "未婚" 
else "未知" end                            as marital_status_tc,

t1.birthday,
if(t1.birthday = '1970-01-01' or t1.birthday is null or t1.birthday = '',0,substr(t1.create_date,1,4)-substr(t1.birthday,1,4)) as age,

t1.employment_form,
case 
when t1.employment_form = 1 then '正式'
when t1.employment_form = 2 then '非正式'
when t1.employment_form = 3 then '实习'
when t1.employment_form = 4 then '劳务'
when t1.employment_form = 5 then '返聘'
when t1.employment_form = 6 then '顾问'
when t1.employment_form = 7 then '派遣'
else '未知' end                             as employment_form_tc,

t1.political_outlook,
t1.graduation_school,
t1.school_attributes,
t1.graduation_date,
t1.major,
t1.first_work_date,

t1.high_major, -- 学历 1初中 2高中 3大专 4本科 5硕士 6博士 7其他
case 
when t1.high_major = 1 then "初中" 
when t1.high_major = 2 then "高中" 
when t1.high_major = 3 then "大专" 
when t1.high_major = 4 then "本科" 
when t1.high_major = 5 then "硕士" 
when t1.high_major = 6 then "博士" 
else "其他" end                            as high_major_tc,

t1.contract,
t1.ascription,
t1.entry_date as orgi_entry_date,
coalesce(t1.entry_date_1,t1.entry_date)    as entry_date,
coalesce(t1.full_date_1,t1.full_date)      as full_date, -- 需要处理 

t1.full_type, -- 转正状态 1未转正 2已转正
case 
when t1.full_type = 1 then "未转正" 
when t1.full_type = 2 then "已转正" 
else "未知" end                            as full_type_tc,

coalesce(t1.offjob_date_1,t1.offjob_date) as offjob_date, -- 计算

t1.first_contract_date,
t1.end_contract_date,
t1.post_id,
t1.post_name,

t1.management_form, -- 管理形式 1总公司 2分公司
case 
when t1.management_form = 1 then "总公司" 
when t1.management_form = 2 then "分公司" 
else "未知" end                            as management_form_tc,

t1.dept_attr, -- 部门属性 1支撑部门 2职能部门 3业务部门
case 
when t1.dept_attr = 1 then "支撑部门" 
when t1.dept_attr = 2 then "职能部门" 
when t1.dept_attr = 3 then "业务部门" 
else "未知" end                                               as dept_attr_tc,

t1.city_id                                                    as city_id,
t5.city_name                                                  as city_name,
t1.adjust_city_id                                             as adjust_city_id, -- 20191104添加 
t6.city_name                                                  as adjust_city_name, -- 20191104添加 

coalesce(t1.job_status_1,t1.job_status)                       as job_status, -- 在职情况  0离职 1在职
case 
when coalesce(t1.job_status_1,t1.job_status) = 1 then "在职" 
when coalesce(t1.job_status_1,t1.job_status) = 0 then "离职" 
else "未知" end                                               as job_status_tc,

t1.direct_leader_id,
t4.employee_name                           as direct_leader_name,
leader_info.direct_leader_id               as indirect_leader_id,
leader_info.direct_leader_name             as indirect_leader_name,

t1.dept_id,
t1.dept_name,

-- 冗余部门表信息 
coalesce(t2.dept_level,t3.dept_level)                as dept_level,
coalesce(t2.team_leader_id,t3.team_leader_id)        as team_leader_id,
coalesce(t2.team_leader_name,t3.team_leader_name)    as team_leader_name,
coalesce(t2.cate_id,t3.cate_id)                      as cate_id,
coalesce(t2.cate_name,t3.cate_name)                  as cate_name,
coalesce(t2.dept_type_id,t3.dept_type_id)            as dept_type_id,
coalesce(t2.dept_type_name,t3.dept_type_name)        as dept_type_name,
coalesce(t2.dept_level_leader,t3.dept_level_leader)  as dept_level_leader,

coalesce(t2.dept_id_first,t3.dept_id_first)          as dept_id_first,
coalesce(t2.dept_name_first,t3.dept_name_first)      as dept_name_first,
coalesce(t2.dept_leader_id_first,t3.dept_leader_id_first)          as dept_leader_id_first,
coalesce(t2.dept_leader_name_first,t3.dept_leader_name_first)      as dept_leader_name_first,

coalesce(t2.dept_id_second,t3.dept_id_second)        as dept_id_second,
coalesce(t2.dept_name_second,t3.dept_name_second)    as dept_name_second,
coalesce(t2.dept_leader_id_second,t3.dept_leader_id_second)        as dept_leader_id_second,
coalesce(t2.dept_leader_name_second,t3.dept_leader_name_second)    as dept_leader_name_second,

coalesce(t2.dept_id_third,t3.dept_id_third)          as dept_id_third,
coalesce(t2.dept_name_third,t3.dept_name_third)      as dept_name_third,
coalesce(t2.dept_leader_id_third,t3.dept_leader_id_third)          as dept_leader_id_third,
coalesce(t2.dept_leader_name_third,t3.dept_leader_name_third)      as dept_leader_name_third,

coalesce(t2.dept_id_fourth,t3.dept_id_fourth)        as dept_id_fourth,
coalesce(t2.dept_name_fourth,t3.dept_name_fourth)    as dept_name_fourth,
coalesce(t2.dept_leader_id_fourth,t3.dept_leader_id_fourth)        as dept_leader_id_fourth,
coalesce(t2.dept_leader_name_fourth,t3.dept_leader_name_fourth)    as dept_leader_name_fourth,

coalesce(t2.dept_id_fifth,t3.dept_id_fifth)          as dept_id_fifth,
coalesce(t2.dept_name_fifth,t3.dept_name_fifth)      as dept_name_fifth,
coalesce(t2.dept_leader_id_fifth,t3.dept_leader_id_fifth)          as dept_leader_id_fifth,
coalesce(t2.dept_leader_name_fifth,t3.dept_leader_name_fifth)      as dept_leader_name_fifth,

coalesce(t2.dept_id_sixth,t3.dept_id_sixth)          as dept_id_sixth,
coalesce(t2.dept_name_sixth,t3.dept_name_sixth)      as dept_name_sixth,
coalesce(t2.dept_leader_id_sixth,t3.dept_leader_id_sixth)          as dept_leader_id_sixth,
coalesce(t2.dept_leader_name_sixth,t3.dept_leader_name_sixth)      as dept_leader_name_sixth,

coalesce(t2.dept_id_seventh,t3.dept_id_seventh)      as dept_id_seventh,
coalesce(t2.dept_name_seventh,t3.dept_name_seventh)  as dept_name_seventh,
coalesce(t2.dept_leader_id_seventh,t3.dept_leader_id_seventh)      as dept_leader_id_seventh,
coalesce(t2.dept_leader_name_seventh,t3.dept_leader_name_seventh)  as dept_leader_name_seventh,

coalesce(t2.dept_id_eighth,t3.dept_id_eighth)        as dept_id_eighth,
coalesce(t2.dept_name_eighth,t3.dept_name_eighth)    as dept_name_eighth,
coalesce(t2.dept_leader_id_eighth,t3.dept_leader_id_eighth)        as dept_leader_id_eighth,
coalesce(t2.dept_leader_name_eighth,t3.dept_leader_name_eighth)    as dept_leader_name_eighth,

t1.create_date,

1 as version,
1 as status,
t1.create_date as start_date,
current_timestamp() as etl_time,
${hiveconf:etl_date} as end_date

from tmp_dev_1.tmp_employee_base_info_test t1 
left join (

select 

a1.emp_id,
a1.direct_leader_id,
a2.employee_name as direct_leader_name

from tmp_dev_1.tmp_employee_base_info_test a1 
left join ods.yw_employee a2 on a1.direct_leader_id = a2.id
where a1.p_date = ${hiveconf:etl_date}

) leader_info on t1.direct_leader_id = leader_info.emp_id 
left join (

select * 
from julive_dim.dim_department_info 
where ${hiveconf:etl_date} >= start_date 
  and ${hiveconf:etl_date} < end_date

) t2 on t1.dept_id = t2.dept_id 
left join (

select * 
from julive_dim.dim_department_info 
where end_date = '9999-12-31'

) t3 on t1.dept_id = t3.dept_id 

left join ods.yw_employee t4 on t1.direct_leader_id = t4.id 
left join julive_dim.dim_city t5 on t1.city_id = t5.city_id 
left join julive_dim.dim_city t6 on t1.adjust_city_id = t6.city_id 

where t1.p_date = ${hiveconf:etl_date}
;


drop table if exists test.tmp_employee_base;
create table test.tmp_employee_base as 

select 

t1.employee_id                                               as emp_id,
t1.employee_name                                             as emp_name,
t1.job_number                                                as job_number,
t1.idcard                                                    as card_no,
t1.sex                                                       as sex,
t1.nation                                                    as nation, -- 民族 
t1.domicile_address                                          as domicile_address, -- 户籍
t1.marital_status                                            as marital_status, -- 婚姻状况
to_date(from_unixtime(t1.birth))                             as birthday, 
t1.age                                                       as age,
t1.employment_form                                           as employment_form, -- 聘用形式
t1.political_outlook                                         as political_outlook, -- 政治面貌
t1.graduation_school                                         as graduation_school, -- 毕业学校
t1.school_attributes                                         as school_attributes, -- 学校等级 
to_date(from_unixtime(t1.graduation_time))                   as graduation_date, -- 毕业日期 
t1.major                                                     as major, -- 专业 
to_date(from_unixtime(t1.first_worktime))                    as first_work_date,
t1.high_major                                                as high_major,
t1.contract                                                  as contract,
t1.ascription                                                as ascription, -- 社保、公积金归属
to_date(from_unixtime(t1.entry_datetime))                    as entry_date, -- 入职日期, 计算 
to_date(from_unixtime(t1.full_time))                         as full_date, -- 转正日期, 计算
t1.full_type                                                 as full_type,
to_date(from_unixtime(t1.offjob_time))                       as offjob_date, -- 离职日期, 计算 
to_date(from_unixtime(t1.first_contract_time))               as first_contract_date,
to_date(from_unixtime(t1.end_contract_time))                 as end_contract_date,
t1.post_id                                                   as post_id,
t1.post_name                                                 as post_name,
t1.management_form                                           as management_form,
t1.department_attributes                                     as dept_attr,
t1.city_id                                                   as city_id,
t1.status                                                    as job_status, -- 员工状态, 计算
t1.direct_leader_id                                          as direct_leader_id,
t1.leader_employee_id                                        as leader_emp_id, -- 20191104添加 
t1.leader_job_number                                         as leader_job_number, -- 20191104添加 
t1.adjust_city_id                                            as adjust_city_id, -- 20191104添加 
t1.department_id                                             as dept_id,
t1.department_name                                           as dept_name,
to_date(from_unixtime(t1.create_datetime))                   as create_date,
max(to_date(from_unixtime(t1.check_date)))                   as check_date  -- 20191104添加 

from (select table1.*
from ods.yw_developer_city_config table2 
left join ods.hr_manpower_monthly  table1   on table1.city_id = table2.city_id) t1 
where t1.employee_id is not null 
and t1.employee_id != 0 
and t1.employee_id != -1 
and to_date(from_unixtime(t1.create_datetime))<=${hiveconf:etl_date}
and t1.create_datetime >=999999999
group by t1.employee_id,
t1.employee_name,
t1.job_number,                                           
t1.idcard,
t1.sex,
t1.nation,
t1.domicile_address,
t1.marital_status,
t1.birth,                        
t1.age,
t1.employment_form,                                      
t1.political_outlook,                                    
t1.graduation_school,                                    
t1.school_attributes,                                    
t1.graduation_time,              
t1.major,                                                
t1.first_worktime,               
t1.high_major,                                           
t1.contract,                                             
t1.ascription,                                           
t1.entry_datetime,               
t1.full_time,                   
t1.full_type,                                            
t1.offjob_time,              
t1.first_contract_time,
t1.end_contract_time,
t1.post_id,                                              
t1.post_name,                                          
t1.management_form,                                      
t1.department_attributes,                               
t1.city_id,                                              
t1.status,                                               
t1.direct_leader_id,                                     
t1.leader_employee_id,                                   
t1.leader_job_number,                                    
t1.adjust_city_id,                                       
t1.department_id,                                        
t1.department_name,                                      
t1.create_datetime
;


drop table if exists tmp_dev_1.tmp_employee_base_info_test;
create table tmp_dev_1.tmp_employee_base_info_test as

select 

emp_id,
emp_name,
job_number,
card_no,
sex,
nation,
domicile_address,
marital_status,
birthday,
age,
employment_form, 
political_outlook,
graduation_school,
school_attributes,
graduation_date,
major,
first_work_date,
high_major,
contract,
ascription,
entry_date,
entry_date_1,
full_date,
full_date_1,
full_type,
offjob_date,
offjob_date_1,
first_contract_date,
end_contract_date,
post_id,
post_name,
management_form,
dept_attr,
city_id,
job_status,
job_status_1,
direct_leader_id,
leader_emp_id, -- 20191104添加 
leader_job_number, -- 20191104添加 
adjust_city_id, -- 20191104添加 
dept_id,
dept_name,
create_date,
check_date, -- 20191104添加 

${hiveconf:etl_date} as p_date 

from (

select 

t1.emp_id,
t1.emp_name,
t1.job_number,
t1.card_no,
t1.sex,
t1.nation,
t1.domicile_address,
t1.marital_status,
t1.birthday,
t1.age,
t1.employment_form,
t1.political_outlook,
t1.graduation_school,
t1.school_attributes,
t1.graduation_date,
t1.major,
t1.first_work_date,
t1.high_major,
t1.contract,
t1.ascription,
t1.full_type,
t1.first_contract_date,
t1.end_contract_date,
t1.post_id,
t1.post_name,
t1.management_form,
t1.dept_attr,
t1.direct_leader_id,
t1.dept_id,
t1.dept_name,
t1.create_date,
t1.leader_emp_id, -- 20191104添加 
t1.leader_job_number, -- 20191104添加 
t1.check_date, -- 20191104添加 

if(t1.city_id in (0,-1),t1.adjust_city_id,t1.city_id)        as city_id, -- 20191104添加 
if(t1.adjust_city_id in (0,-1),t1.city_id,t1.adjust_city_id) as adjust_city_id, -- 20191104添加 

t1.entry_date, -- 计算 ,没毛病 
min(t1.create_date)over(partition by t1.emp_id) as entry_date_1,

t1.full_date, -- 计算 ，后续需要对 9999-12-31 进行处理 
min(if(t1.full_type = 2,t1.create_date,'9999-99-99'))over(partition by t1.emp_id) as full_date_1,


-- 计算属性 
-- 删除场景：操作员工离职，会将离职日期之后的数据删掉。
t1.job_status, -- 计算 , 没毛病 
if(t1.create_date = ${hiveconf:etl_date},1,0) as job_status_1,

if(t1.create_date is not null and t1.create_date<${hiveconf:etl_date},t1.create_date,t1.offjob_date) as offjob_date, -- 计算 ,离职人计算结果正确，在职需要根据job_status处理
max(if(t1.create_date = ${hiveconf:etl_date},'9999-99-99',t1.create_date))over(partition by t1.emp_id) as offjob_date_1,


row_number()over(partition by t1.emp_id order by t1.create_date desc) as rn 

from test.tmp_employee_base t1 
where t1.create_date <= ${hiveconf:etl_date} 

) t 
where t.rn = 1 
;




insert overwrite table julive_dim.dim_wlmq_employee_info partition(p_date =${hiveconf:etl_date}) 

select 

regexp_replace(uuid(),'-','')             as skey,

t1.emp_id,
t1.emp_name,
t1.job_number,
t1.card_no ,

t1.sex,
case 
when t1.sex = 1 then "男" 
when t1.sex = 2 then "女"
else "未知" end                            as sex_tc,

t1.nation,
t1.domicile_address,

t1.marital_status,
case 
when t1.marital_status = 1 then "已婚" 
when t1.marital_status = 2 then "未婚" 
else "未知" end                            as marital_status_tc,

t1.birthday,
if(t1.birthday = '1970-01-01' or t1.birthday is null or t1.birthday = '',0,substr(t1.create_date,1,4)-substr(t1.birthday,1,4)) as age,

t1.employment_form,
case 
when t1.employment_form = 1 then '正式'
when t1.employment_form = 2 then '非正式'
when t1.employment_form = 3 then '实习'
when t1.employment_form = 4 then '劳务'
when t1.employment_form = 5 then '返聘'
when t1.employment_form = 6 then '顾问'
when t1.employment_form = 7 then '派遣'
else '未知' end                             as employment_form_tc,

t1.political_outlook,
t1.graduation_school,
t1.school_attributes,
t1.graduation_date,
t1.major,
t1.first_work_date,

t1.high_major, -- 学历 1初中 2高中 3大专 4本科 5硕士 6博士 7其他
case 
when t1.high_major = 1 then "初中" 
when t1.high_major = 2 then "高中" 
when t1.high_major = 3 then "大专" 
when t1.high_major = 4 then "本科" 
when t1.high_major = 5 then "硕士" 
when t1.high_major = 6 then "博士" 
else "其他" end                            as high_major_tc,

t1.contract,
t1.ascription,
t1.entry_date as orgi_entry_date,
coalesce(t1.entry_date_1,t1.entry_date)    as entry_date,
coalesce(t1.full_date_1,t1.full_date)      as full_date, -- 需要处理 

t1.full_type, -- 转正状态 1未转正 2已转正
case 
when t1.full_type = 1 then "未转正" 
when t1.full_type = 2 then "已转正" 
else "未知" end                            as full_type_tc,

coalesce(t1.offjob_date_1,t1.offjob_date) as offjob_date, -- 计算

t1.first_contract_date,
t1.end_contract_date,
t1.post_id,
t1.post_name,

t1.management_form, -- 管理形式 1总公司 2分公司
case 
when t1.management_form = 1 then "总公司" 
when t1.management_form = 2 then "分公司" 
else "未知" end                            as management_form_tc,

t1.dept_attr, -- 部门属性 1支撑部门 2职能部门 3业务部门
case 
when t1.dept_attr = 1 then "支撑部门" 
when t1.dept_attr = 2 then "职能部门" 
when t1.dept_attr = 3 then "业务部门" 
else "未知" end                                               as dept_attr_tc,

t1.city_id                                                    as city_id,
t5.city_name                                                  as city_name,
t1.adjust_city_id                                             as adjust_city_id, -- 20191104添加 
t6.city_name                                                  as adjust_city_name, -- 20191104添加 

coalesce(t1.job_status_1,t1.job_status)                       as job_status, -- 在职情况  0离职 1在职
case 
when coalesce(t1.job_status_1,t1.job_status) = 1 then "在职" 
when coalesce(t1.job_status_1,t1.job_status) = 0 then "离职" 
else "未知" end                                               as job_status_tc,

t1.direct_leader_id,
t4.employee_name                           as direct_leader_name,
leader_info.direct_leader_id               as indirect_leader_id,
leader_info.direct_leader_name             as indirect_leader_name,

t1.dept_id,
t1.dept_name,

-- 冗余部门表信息 
coalesce(t2.dept_level,t3.dept_level)                as dept_level,
coalesce(t2.team_leader_id,t3.team_leader_id)        as team_leader_id,
coalesce(t2.team_leader_name,t3.team_leader_name)    as team_leader_name,
coalesce(t2.cate_id,t3.cate_id)                      as cate_id,
coalesce(t2.cate_name,t3.cate_name)                  as cate_name,
coalesce(t2.dept_type_id,t3.dept_type_id)            as dept_type_id,
coalesce(t2.dept_type_name,t3.dept_type_name)        as dept_type_name,
coalesce(t2.dept_level_leader,t3.dept_level_leader)  as dept_level_leader,

coalesce(t2.dept_id_first,t3.dept_id_first)          as dept_id_first,
coalesce(t2.dept_name_first,t3.dept_name_first)      as dept_name_first,
coalesce(t2.dept_leader_id_first,t3.dept_leader_id_first)          as dept_leader_id_first,
coalesce(t2.dept_leader_name_first,t3.dept_leader_name_first)      as dept_leader_name_first,

coalesce(t2.dept_id_second,t3.dept_id_second)        as dept_id_second,
coalesce(t2.dept_name_second,t3.dept_name_second)    as dept_name_second,
coalesce(t2.dept_leader_id_second,t3.dept_leader_id_second)        as dept_leader_id_second,
coalesce(t2.dept_leader_name_second,t3.dept_leader_name_second)    as dept_leader_name_second,

coalesce(t2.dept_id_third,t3.dept_id_third)          as dept_id_third,
coalesce(t2.dept_name_third,t3.dept_name_third)      as dept_name_third,
coalesce(t2.dept_leader_id_third,t3.dept_leader_id_third)          as dept_leader_id_third,
coalesce(t2.dept_leader_name_third,t3.dept_leader_name_third)      as dept_leader_name_third,

coalesce(t2.dept_id_fourth,t3.dept_id_fourth)        as dept_id_fourth,
coalesce(t2.dept_name_fourth,t3.dept_name_fourth)    as dept_name_fourth,
coalesce(t2.dept_leader_id_fourth,t3.dept_leader_id_fourth)        as dept_leader_id_fourth,
coalesce(t2.dept_leader_name_fourth,t3.dept_leader_name_fourth)    as dept_leader_name_fourth,

coalesce(t2.dept_id_fifth,t3.dept_id_fifth)          as dept_id_fifth,
coalesce(t2.dept_name_fifth,t3.dept_name_fifth)      as dept_name_fifth,
coalesce(t2.dept_leader_id_fifth,t3.dept_leader_id_fifth)          as dept_leader_id_fifth,
coalesce(t2.dept_leader_name_fifth,t3.dept_leader_name_fifth)      as dept_leader_name_fifth,

coalesce(t2.dept_id_sixth,t3.dept_id_sixth)          as dept_id_sixth,
coalesce(t2.dept_name_sixth,t3.dept_name_sixth)      as dept_name_sixth,
coalesce(t2.dept_leader_id_sixth,t3.dept_leader_id_sixth)          as dept_leader_id_sixth,
coalesce(t2.dept_leader_name_sixth,t3.dept_leader_name_sixth)      as dept_leader_name_sixth,

coalesce(t2.dept_id_seventh,t3.dept_id_seventh)      as dept_id_seventh,
coalesce(t2.dept_name_seventh,t3.dept_name_seventh)  as dept_name_seventh,
coalesce(t2.dept_leader_id_seventh,t3.dept_leader_id_seventh)      as dept_leader_id_seventh,
coalesce(t2.dept_leader_name_seventh,t3.dept_leader_name_seventh)  as dept_leader_name_seventh,

coalesce(t2.dept_id_eighth,t3.dept_id_eighth)        as dept_id_eighth,
coalesce(t2.dept_name_eighth,t3.dept_name_eighth)    as dept_name_eighth,
coalesce(t2.dept_leader_id_eighth,t3.dept_leader_id_eighth)        as dept_leader_id_eighth,
coalesce(t2.dept_leader_name_eighth,t3.dept_leader_name_eighth)    as dept_leader_name_eighth,

t1.create_date,

1 as version,
1 as status,
t1.create_date as start_date,
current_timestamp() as etl_time,
${hiveconf:etl_date} as end_date

from tmp_dev_1.tmp_employee_base_info_test t1 
left join (

select 

a1.emp_id,
a1.direct_leader_id,
a2.employee_name as direct_leader_name

from tmp_dev_1.tmp_employee_base_info_test a1 
left join ods.yw_employee a2 on a1.direct_leader_id = a2.id
where a1.p_date = ${hiveconf:etl_date}

) leader_info on t1.direct_leader_id = leader_info.emp_id 
left join (

select * 
from julive_dim.dim_department_info 
where ${hiveconf:etl_date} >= start_date 
  and ${hiveconf:etl_date} < end_date

) t2 on t1.dept_id = t2.dept_id 
left join (

select * 
from julive_dim.dim_department_info 
where end_date = '9999-12-31'

) t3 on t1.dept_id = t3.dept_id 

left join ods.yw_employee t4 on t1.direct_leader_id = t4.id 
left join julive_dim.dim_wlmq_city t5 on t1.city_id = t5.city_id 
left join julive_dim.dim_wlmq_city t6 on t1.adjust_city_id = t6.city_id 

where t1.p_date = ${hiveconf:etl_date}
;
