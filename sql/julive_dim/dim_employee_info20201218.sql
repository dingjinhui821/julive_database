set mapred.job.name=dim_esf_employee_info;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table julive_dim.dim_wlmq_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add('20201216',-5) 
and from_source = 2 
and emp_name not like '%测试%' 
;


-- 6.3 产出二手房中介子表 
insert overwrite table julive_dim.dim_esf_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add('20201216',-5) 
and from_source = 3 
and emp_name not like '%测试%' 
;


-- 6.4 产出智能AI客服员工数据 
insert overwrite table julive_dim.dim_ai_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add('20201216',-5)
and from_source = 1 and is_ai_service =1
;



--6.6加盟商数据
insert overwrite table julive_dim.dim_jms_employee_info partition(pdate) 

select *
from julive_dim.dim_employee_base_info
where pdate >= date_add('20201216',-5) 
and from_source = 4 
;
