-- 依赖 
-- ods.hr_manpower_monthly 
-- ods.yw_employee 
-- julive_dim.dim_department_info
-- ods.yw_developer_city_config



-- drop table if exists julive_dim.dim_employee_info;
-- create external table julive_dim.dim_employee_info
-- like julive_dim.dim_employee_base_info;
-- 
-- drop table if exists julive_dim.dim_ai_employee_info;
-- create external table julive_dim.dim_ai_employee_info
-- like julive_dim.dim_employee_base_info;
-- 
-- drop table if exists julive_dim.dim_wlmq_employee_info;
-- create external table julive_dim.dim_wlmq_employee_info
-- like julive_dim.dim_employee_base_info;
-- 
-- drop table if exists julive_dim.dim_esf_employee_info;
-- create external table julive_dim.dim_esf_employee_info
-- like julive_dim.dim_employee_base_info;
-- 
-- drop table if exists julive_dim.dim_jms_employee_info;
-- create external table julive_dim.dim_jms_employee_info
-- like julive_dim.dim_employee_base_info;

-- drop table if exists julive_dim.dim_consultant_info;
-- create external table julive_dim.dim_consultant_info
-- like julive_dim.dim_employee_base_info;

-- 
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ETL脚本 
-- job 1 : 处理基础数据信息 
-- set etl_date = date_add(current_date(),-1);
-- set etl_yestoday = date_add(current_date(),-2);

set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
set etl_yestoday = concat_ws('-',substr(${hiveconf:etlYestoday},1,4),substr(${hiveconf:etlYestoday},5,2),substr(${hiveconf:etlYestoday},7,2));

set spark.app.name=dim_employee_base_info;
set mapred.job.name=dim_employee_base_info;

set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


-- ----------------------------------------------------------------------------------
-- 1 解析当天采集的人力月报表数据 
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
to_date(from_unixtime(t1.check_date))                        as check_date,  -- 20191104添加
t1.org_id

from ods.hr_manpower_monthly t1 
where t1.employee_id is not null 
and t1.employee_id != 0 
and t1.employee_id != -1 
and t1.create_datetime > 999999999 
-- and t1.employee_name not like '%测试%' -- 20200917添加 
;


-- ----------------------------------------------------------------------------------
-- 2 计算当天数据快照 
insert overwrite table tmp_dev_1.tmp_employee_base_info partition(p_date) 

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
org_id,

${hiveconf:etl_date} as p_date -- yyyy-MM-dd 

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

t1.offjob_date, -- 计算 ,离职人计算结果正确，在职需要根据job_status处理
max(if(t1.create_date = ${hiveconf:etl_date},'9999-99-99',t1.create_date))over(partition by t1.emp_id) as offjob_date_1,

row_number()over(partition by t1.emp_id order by t1.create_date desc) as rn,
t1.org_id 

from test.tmp_employee_base t1 
where t1.create_date <= ${hiveconf:etl_date} 
and t1.create_date >= '2010-01-01' 

) t 
where t.rn = 1 
;


-- -----------------------------------------------------------------------------------
-- 3 处理当前最新数据 
insert overwrite table julive_dim.dim_employee_base_info partition(pdate) 

select 

regexp_replace(uuid(),'-','')                                      as skey,
t1.org_id                                                          as org_id,
t11.org_type                                                       as org_type,
t1.emp_id                                                          as emp_id,
t1.emp_name                                                        as emp_name,

-- if(t7.id is null and t9.id is null,1, -- 自营 
--    if(t7.id is not null,2, -- 乌鲁木齐 
--       if(t9.id is not null,3, -- 二手房中介 
--   999)))                                as from_source,  -- 20200917更改-添加了AI客服 

-- case 
-- when t7.id is null and t9.id is null and t10.dept_id is null then 1 -- 自营 
-- when t7.id is not null then 2 -- 乌鲁木齐 
-- when t9.id is not null then 3 -- 二手房中介 
-- when t10.dept_id is not null then 11 -- 智能AI客服 
-- else 999 -- 其他 
-- end                                                                as from_source,  -- 20200917更改-添加了AI客服 

case 
when t7.id is not null then 2 -- 乌鲁木齐数据 
when t9.id is not null then 3 -- 二手房中介数据 
when t10.dept_id is not null then 1 -- 智能AI客服 
when (t11.org_type != 1 and t11.id is not null) then 4 -- 加盟商数据 
else 1 -- 居理数据 
end                                                                as from_source,
if( t10.dept_id is not null ,1,0)                                  as is_ai_service,

t1.job_number                                                      as job_number,
t1.card_no                                                         as card_no,
t1.sex                                                             as sex,
case 
when t1.sex = 1 then "男" 
when t1.sex = 2 then "女"
else "未知" end                                                    as sex_tc,
t1.nation                                                          as nation,
t1.domicile_address                                                as domicile_address,
t1.marital_status                                                  as marital_status,
case 
when t1.marital_status = 1 then "已婚" 
when t1.marital_status = 2 then "未婚" 
else "未知" end                                                    as marital_status_tc,
t1.birthday                                                        as birthday,
if(t1.birthday = '1970-01-01' 
or t1.birthday is null 
or t1.birthday = '',0,
substr(t1.create_date,1,4)-substr(t1.birthday,1,4))                as age,
t1.employment_form                                                 as employment_form,
case 
when t1.employment_form = 1 then '正式'
when t1.employment_form = 2 then '非正式'
when t1.employment_form = 3 then '实习'
when t1.employment_form = 4 then '劳务'
when t1.employment_form = 5 then '返聘'
when t1.employment_form = 6 then '顾问'
when t1.employment_form = 7 then '派遣'
else '未知' end                                                    as employment_form_tc,
t1.political_outlook                                               as political_outlook,
t1.graduation_school                                               as graduation_school,
t1.school_attributes                                               as school_attributes,
t1.graduation_date                                                 as graduation_date,
t1.major                                                           as major,
t1.first_work_date                                                 as first_work_date,
t1.high_major                                                      as high_major, -- 学历 1初中 2高中 3大专 4本科 5硕士 6博士 7其他
case 
when t1.high_major = 1 then "初中" 
when t1.high_major = 2 then "高中" 
when t1.high_major = 3 then "大专" 
when t1.high_major = 4 then "本科" 
when t1.high_major = 5 then "硕士" 
when t1.high_major = 6 then "博士" 
else "其他" end                                                    as high_major_tc,
t1.contract                                                        as contract,
t1.ascription                                                      as ascription,
t1.entry_date                                                      as orgi_entry_date, -- 20200423
coalesce(t1.entry_date_1,t1.entry_date)                            as entry_date,
coalesce(t1.full_date_1,t1.full_date)                              as full_date, -- 需要处理 
t1.full_type                                                       as full_type, -- 转正状态 1未转正 2已转正
case 
when t1.full_type = 1 then "未转正" 
when t1.full_type = 2 then "已转正" 
else "未知" end                                                    as full_type_tc,
coalesce(t1.offjob_date_1,t1.offjob_date)                          as offjob_date, -- 计算
t1.first_contract_date                                             as first_contract_date, 
t1.end_contract_date                                               as end_contract_date,
t1.post_id                                                         as post_id,
t1.post_name                                                       as post_name,
t1.management_form                                                 as management_form, -- 管理形式 1总公司 2分公司
case 
when t1.management_form = 1 then "总公司" 
when t1.management_form = 2 then "分公司" 
else "未知" end                                                    as management_form_tc,
t1.dept_attr                                                       as dept_attr, -- 部门属性 1支撑部门 2职能部门 3业务部门
case 
when t1.dept_attr = 1 then "支撑部门" 
when t1.dept_attr = 2 then "职能部门" 
when t1.dept_attr = 3 then "业务部门" 
else "未知" end                                                    as dept_attr_tc,
t1.city_id                                                         as city_id,
if(t5.city_name is not null,t5.city_name,t8.city_name)             as city_name,
t1.adjust_city_id                                                  as adjust_city_id, -- 20191104添加 

t6.city_name                                                       as adjust_city_name, -- 20191104添加 
coalesce(t1.job_status_1,t1.job_status)                            as job_status, -- 在职情况  0离职 1在职
case 
when coalesce(t1.job_status_1,t1.job_status) = 1 then "在职" 
when coalesce(t1.job_status_1,t1.job_status) = 0 then "离职" 
else "未知" end                                                    as job_status_tc,
t1.direct_leader_id                                                as direct_leader_id,
t4.employee_name                                                   as direct_leader_name,
leader_info.direct_leader_id                                       as indirect_leader_id,
leader_info.direct_leader_name                                     as indirect_leader_name,
t1.dept_id                                                         as dept_id,
t1.dept_name                                                       as dept_name,
-- 冗余部门表信息 
coalesce(t2.dept_level,t3.dept_level)                              as dept_level,
coalesce(t2.team_leader_id,t3.team_leader_id)                      as team_leader_id,
coalesce(t2.team_leader_name,t3.team_leader_name)                  as team_leader_name,
coalesce(t2.cate_id,t3.cate_id)                                    as cate_id,
coalesce(t2.cate_name,t3.cate_name)                                as cate_name,
coalesce(t2.dept_type_id,t3.dept_type_id)                          as dept_type_id,
coalesce(t2.dept_type_name,t3.dept_type_name)                      as dept_type_name,
coalesce(t2.dept_level_leader,t3.dept_level_leader)                as dept_level_leader,

coalesce(t2.dept_id_first,t3.dept_id_first)                        as dept_id_first,
coalesce(t2.dept_name_first,t3.dept_name_first)                    as dept_name_first,
coalesce(t2.dept_leader_id_first,t3.dept_leader_id_first)          as dept_leader_id_first,
coalesce(t2.dept_leader_name_first,t3.dept_leader_name_first)      as dept_leader_name_first,

coalesce(t2.dept_id_second,t3.dept_id_second)                      as dept_id_second,
coalesce(t2.dept_name_second,t3.dept_name_second)                  as dept_name_second,
coalesce(t2.dept_leader_id_second,t3.dept_leader_id_second)        as dept_leader_id_second,
coalesce(t2.dept_leader_name_second,t3.dept_leader_name_second)    as dept_leader_name_second,

coalesce(t2.dept_id_third,t3.dept_id_third)                        as dept_id_third,
coalesce(t2.dept_name_third,t3.dept_name_third)                    as dept_name_third,
coalesce(t2.dept_leader_id_third,t3.dept_leader_id_third)          as dept_leader_id_third,
coalesce(t2.dept_leader_name_third,t3.dept_leader_name_third)      as dept_leader_name_third,

coalesce(t2.dept_id_fourth,t3.dept_id_fourth)                      as dept_id_fourth,
coalesce(t2.dept_name_fourth,t3.dept_name_fourth)                  as dept_name_fourth,
coalesce(t2.dept_leader_id_fourth,t3.dept_leader_id_fourth)        as dept_leader_id_fourth,
coalesce(t2.dept_leader_name_fourth,t3.dept_leader_name_fourth)    as dept_leader_name_fourth,

coalesce(t2.dept_id_fifth,t3.dept_id_fifth)                        as dept_id_fifth,
coalesce(t2.dept_name_fifth,t3.dept_name_fifth)                    as dept_name_fifth,
coalesce(t2.dept_leader_id_fifth,t3.dept_leader_id_fifth)          as dept_leader_id_fifth,
coalesce(t2.dept_leader_name_fifth,t3.dept_leader_name_fifth)      as dept_leader_name_fifth,

coalesce(t2.dept_id_sixth,t3.dept_id_sixth)                        as dept_id_sixth,
coalesce(t2.dept_name_sixth,t3.dept_name_sixth)                    as dept_name_sixth,
coalesce(t2.dept_leader_id_sixth,t3.dept_leader_id_sixth)          as dept_leader_id_sixth,
coalesce(t2.dept_leader_name_sixth,t3.dept_leader_name_sixth)      as dept_leader_name_sixth,

coalesce(t2.dept_id_seventh,t3.dept_id_seventh)                    as dept_id_seventh,
coalesce(t2.dept_name_seventh,t3.dept_name_seventh)                as dept_name_seventh,
coalesce(t2.dept_leader_id_seventh,t3.dept_leader_id_seventh)      as dept_leader_id_seventh,
coalesce(t2.dept_leader_name_seventh,t3.dept_leader_name_seventh)  as dept_leader_name_seventh,

coalesce(t2.dept_id_eighth,t3.dept_id_eighth)                      as dept_id_eighth,
coalesce(t2.dept_name_eighth,t3.dept_name_eighth)                  as dept_name_eighth,
coalesce(t2.dept_leader_id_eighth,t3.dept_leader_id_eighth)        as dept_leader_id_eighth,
coalesce(t2.dept_leader_name_eighth,t3.dept_leader_name_eighth)    as dept_leader_name_eighth,

t1.create_date                                                     as create_date,
''                                                                 as version, -- 无效字段 
1                                                                  as status, -- 1 当前装填 0 历史数据 
${hiveconf:etl_date}                                               as start_date, -- 当天生成数据的日期作为开始日期 
current_timestamp()                                                as etl_time,
'9999-12-31'                                                       as end_date, -- 未来日期作为结束日期 

regexp_replace(${hiveconf:etl_date},'-','')                        as pdate -- 分区日期 yyyyMMdd 

from ( 

select * 
from tmp_dev_1.tmp_employee_base_info 
where p_date = ${hiveconf:etl_date}

) t1 left join (

select 

a1.emp_id,
a1.direct_leader_id,
a2.employee_name as direct_leader_name

from tmp_dev_1.tmp_employee_base_info a1 
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
left join ods.yw_developer_city_config t7 on t1.city_id = t7.city_id -- 乌鲁木齐 
left join julive_dim.dim_wlmq_city t8 on t1.city_id = t8.city_id 
left join ods.yw_esf_virtual_config t9 on t1.city_id = t9.virtual_city -- 二手房中介 

left join ( -- 智能AI客服 

select t1.dept_id,t1.dept_name 
from julive_dim.dim_department_info t1 
where t1.end_date = '9999-12-31'  
AND ( 
t1.dept_name like '%智能AI%' or 
t1.dept_name_first like '%智能AI%' or 
t1.dept_name_second like '%智能AI%' or 
t1.dept_name_third like '%智能AI%' or 
t1.dept_name_fourth like '%智能AI%' or 
t1.dept_name_fifth like '%智能AI%' or 
t1.dept_name_sixth like '%智能AI%' or 
t1.dept_name_seventh like '%智能AI%' or 
t1.dept_name_eighth like '%智能AI%' 
) 

) t10 on t1.dept_id = t10.dept_id 
left join ods.yw_org_info t11 on t1.org_id = t11.org_id
;


-- -----------------------------------------------------------------------------------
-- 4 刷新昨天的数据，end_date 模拟闭链 
-- 刷历史 需要处理的数据 ：
-- offjob_date                  string    comment '离职时间',
-- job_status                   int       comment '在职情况  0离职 1在职',
-- job_status_tc                string    comment '在职情况  0离职 1在职',

set spark.app.name=dim_employee_info;
set hive.execution.engine=mr;
set mapreduce.job.reduces=8;
set spark.default.parallelism=1400;
set spark.executor.cores=4;
set spark.executor.memory=8g;
set spark.executor.instances=14;
set spark.yarn.executor.memoryOverhead=2048;
set hive.prewarm.enabled=true;
set hive.prewarm.numcontainers=14;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;
-------基础设置
set hive.exec.orc.default.block.size=134217728;
set mapreduce.input.fileinputformat.split.maxsize = 100000000;
set hive.auto.convert.join=true;
set hive.exec.compress.intermediate=true;
set hive.exec.reducers.bytes.per.reducer=500000000;
-----内存设置
set mapreduce.reduce.memory.mb=8192;
set mapreduce.map.memory.mb=8192;
set mapreduce.task.io.sort.mb=800;
-- set mapreduce.reduce.java.opts=-Djava.net.preferIPv4Stack=true -Xmx3200m;
set mapreduce.reduce.shuffle.parallelcopies=20;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
-----输出合并小文件
set hive.merge.mapfiles = true;
-- set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 128000000;
-- set hive.merge.smallfiles.avgsize=30000000;
-----输入合并小文件
set mapred.max.split.size=128000000; 
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
set mapreduce.input.fileinputformat.split.minsize.per.rack=11000000;


drop table if exists test.tmp_employee_90;
create table test.tmp_employee_90 as 

select 

t1.skey,
t1.org_id,   
t1.org_type,
t1.emp_id,
t1.emp_name,
t1.from_source,
t1.is_ai_service,
t1.job_number,
t1.card_no,
t1.sex,
t1.sex_tc,
t1.nation,
t1.domicile_address,
t1.marital_status,
t1.marital_status_tc,
t1.birthday,
t1.age,
t1.employment_form,
t1.employment_form_tc,
t1.political_outlook,
t1.graduation_school,
t1.school_attributes,
t1.graduation_date,
t1.major,
t1.first_work_date,
t1.high_major,
t1.high_major_tc,
t1.contract,
t1.ascription,

t1.orgi_entry_date, 
t1.entry_date,
t1.full_date,
t1.full_type,
t1.full_type_tc,

-- t1.offjob_date,  -- 刷新 
if(t1.pdate > regexp_replace(t2.create_date,'-',''),t2.create_date,'9999-99-99') as offjob_date,

t1.first_contract_date,
t1.end_contract_date,
t1.post_id,
t1.post_name,
t1.management_form,
t1.management_form_tc,
t1.dept_attr,
t1.dept_attr_tc,
t1.city_id,
t1.city_name,
t1.adjust_city_id,
t1.adjust_city_name,

-- t1.job_status, -- 刷新 
-- t1.job_status_tc, -- 刷新 
if(t1.pdate > regexp_replace(t2.create_date,'-',''),0,1)           as job_status, -- 1-在职 0-离职 
if(t1.pdate > regexp_replace(t2.create_date,'-',''),'离职','在职') as job_status_tc, -- 1-在职 0-离职 

t1.direct_leader_id,
t1.direct_leader_name,
t1.indirect_leader_id,
t1.indirect_leader_name,
t1.dept_id,
t1.dept_name,
t1.dept_level,
t1.team_leader_id,
t1.team_leader_name,
t1.cate_id,
t1.cate_name,
t1.dept_type_id,
t1.dept_type_name,
t1.dept_level_leader,
t1.dept_id_first,
t1.dept_name_first,
t1.dept_leader_id_first,
t1.dept_leader_name_first,
t1.dept_id_second,
t1.dept_name_second,
t1.dept_leader_id_second,
t1.dept_leader_name_second,
t1.dept_id_third,
t1.dept_name_third,
t1.dept_leader_id_third,
t1.dept_leader_name_third,
t1.dept_id_fourth,
t1.dept_name_fourth,
t1.dept_leader_id_fourth,
t1.dept_leader_name_fourth,
t1.dept_id_fifth,
t1.dept_name_fifth,
t1.dept_leader_id_fifth,
t1.dept_leader_name_fifth,
t1.dept_id_sixth,
t1.dept_name_sixth,
t1.dept_leader_id_sixth,
t1.dept_leader_name_sixth,
t1.dept_id_seventh,
t1.dept_name_seventh,
t1.dept_leader_id_seventh,
t1.dept_leader_name_seventh,
t1.dept_id_eighth,
t1.dept_name_eighth,
t1.dept_leader_id_eighth,
t1.dept_leader_name_eighth,
t1.create_date,

'' as version, -- 版本号无效 
0  as status, -- 均为历史数据 
t1.start_date as start_date, -- 模拟拉链开始日期 
current_timestamp() as etl_time, -- ETL跑数日期 
date_add(t1.start_date,1) as end_date, -- 模拟闭链日期 
t1.pdate 


from julive_dim.dim_employee_base_info t1 
left join tmp_dev_1.tmp_employee_base_info t2 on t1.emp_id = t2.emp_id and t2.p_date = ${hiveconf:etl_date} 

where t1.pdate <= regexp_replace(${hiveconf:etl_yestoday},'-','') 
and t1.pdate >= regexp_replace(date_add(${hiveconf:etl_yestoday},-90),'-','') 
;


-- 5 刷新数据 
insert overwrite table julive_dim.dim_employee_base_info partition(pdate) 
select * 
from test.tmp_employee_90 
;


-- ------------------------------------------------------------------------------------
-- 6 产出子表 

-- 6.1 产出业务线子表
insert overwrite table julive_dim.dim_employee_info partition(pdate) 

select * 
from julive_dim.dim_employee_base_info 
where pdate >= date_add(${hiveconf:etl_yestoday},-90) 
and from_source = 1  and is_ai_service = 0
and emp_name not like '%测试%' 
;


-- 6.2 产出乌鲁木齐子表 
insert overwrite table julive_dim.dim_wlmq_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add(${hiveconf:etl_yestoday},-90) 
and from_source = 2 
and emp_name not like '%测试%' 
;


-- 6.3 产出二手房中介子表 
insert overwrite table julive_dim.dim_esf_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add(${hiveconf:etl_yestoday},-90) 
and from_source = 3 
and emp_name not like '%测试%' 
;


-- 6.4 产出智能AI客服员工数据 
insert overwrite table julive_dim.dim_ai_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add(${hiveconf:etl_yestoday},-90)
and from_source = 1 and is_ai_service =1
;


-- 6.5 产出咨询师维度表 
insert overwrite table julive_dim.dim_consultant_info partition(pdate) 

select * 
from julive_dim.dim_employee_base_info 
where pdate >= date_add(${hiveconf:etl_yestoday},-90) 
and from_source = 1 
and post_id in (132,164,261,133,204,260,162,258,131,163,259) 
and emp_name not like '%测试%'  
;


--6.6加盟商数据
insert overwrite table julive_dim.dim_jms_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add(${hiveconf:etl_yestoday},-90) 
and from_source = 4 
;
 
