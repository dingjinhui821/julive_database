set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=app_xpt_data_complement;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

--xpt_order关联出的所有global_id
drop table if exists test.test01_comjia_unique_id_base;
create table test.test01_comjia_unique_id_base as 

select 
t1.user_id  as user_id,
max(t2.comjia_unique_id) as comjia_unique_id

from ods.xpt_order t1
join dwd.dwd_unique_julive_id_mapping_all t2 on t1.user_id = t2.julive_id
group by t1.user_id
;


--最近三十天所有数据
drop table if exists test.test01_app_xpt_data_complement ;
 create table test.test01_app_xpt_data_complement as
 select
 * from julive_app.app_xpt_data_base
 where pdate >from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(${hiveconf:etl_date},'yyyyMMdd'),'yyyy-MM-dd'),-30),'yyyy-MM-dd'),'yyyyMMdd')
 ;


--
drop table if exists test.test02_app_xpt_data_complement;
create table test.test02_app_xpt_data_complement as  

select 

if(t1.global_id is not null,t1.global_id,'')                    as global_id,
if(t1.comjia_unique_id is not null,t1.comjia_unique_id,'')      as comjia_unique_id,
if(t1.object is not null,t1.object,'')                          as object,
if(t1.operation is not null,t1.operation,'')                    as operation,
if(t1.target_id is not null,t1.target_id,'')                    as target_id,
if(t1.target_type  is not null,t1.target_type ,'')              as target_type,
if(t1.comjia_platform_id is not null,t1.comjia_platform_id,'')  as comjia_platform_id ,
if(t1.product_id is not null,t1.product_id,'')                  as product_id,
if(t1.frompage is not null,t1.frompage,'')                      as frompage,
if(t1.optype_id is not null,t1.optype_id,'')                    as optype_id,
if(t1.agent_id is not null,t1.agent_id,'')                      as agent_id,
if(t1.agent_login_id  is not null,t1.agent_login_id ,'')        as agent_login_id,
if(t1.customer_id is null or t1.customer_id = '' or t1.customer_id in ('0','-1'),t2.user_id,t1.customer_id) as customer_id,
if(t1.openid is not null,t1.openid,'')                          as openid,
if(t1.system is not null,t1.system,'')                          as system,
if(t1.network is not null,t1.network,'')                        as network,
if(t1.operate_time is not null,t1.operate_time,'')              as operate_time 
from test.test01_app_xpt_data_complement t1 
left join test.test01_comjia_unique_id_base t2 on t1.comjia_unique_id = t2.comjia_unique_id 
where if(t1.customer_id is null or t1.customer_id = '' or t1.customer_id in ('0','-1'),t2.user_id,t1.customer_id) > 0 
;

--过滤研发已保存所有数据

drop table if exists julive_app.app_xpt_data_complement;
create table julive_app.app_xpt_data_complement as

select

t1.*
from 
test.test02_app_xpt_data_complement t1

left join ods.xpt_customer_behavior_base t2
on t1.customer_id = t2.customer_id and t1.operate_time  = t2.operate_time

where t2.customer_id is null and t2.operate_time is null;



