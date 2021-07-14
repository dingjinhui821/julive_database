set hive.execution.engine=spark;
set spark.app.name=fact_subscribe_no_sign_nowsign_indi;
set spark.yarn.queue=etl;
Insert overwrite table julive_fact.fact_subscribe_no_sign_nowsign_indi
select
a1.deal_id,                      
a1.subscribe_id,                 
a1.grass_sign_id,                
a1.clue_id,                      
                             
a1.project_city_id,              
a1.project_city_name,            
a1.project_city_seq,             
a1.mgr_city_name,                
a1.mgr_city_seq,                 

a1.project_id,                   
a1.project_name,                 
a1.project_type,                 
                             
a1.emp_id,                       
a1.emp_name,                     
a1.clue_emp_id,                  
a1.clue_emp_name,                

a1.direct_leader_id,             
a1.direct_leader_name,           
a1.indirect_leader_id,           
a1.indirect_leader_name,         
a1.now_direct_leader_id,         
a1.now_direct_leader_name,       
a1.now_indirect_leader_id,       
a1.now_indirect_leader_name,     

a1.subscribe_status,             
a1.subscribe_type,               
a1.grass_sign_status,            

a1.is_have_risk,                 


a1.receive_amt,                  
a1.actual_amt,                   


a1.subscribe_date,              
a1.actual_date,                  
a1.grass_sign_date,              
a1.back_date,                    
a1.plan_sign_date,               

a1.pdate,

t3.sign_id,                      
t3.sign_date,                    

current_timestamp() as etl_time                     

from julive_fact.fact_subscribe_no_sign_indi a1
left join 
(select
t2.sign_id,
t2.deal_id,
t2.sign_date
from

(select 

 t1.id as sign_id,
 t1.deal_id,
 from_unixtime(t1.sign_datetime,"yyyy-MM-dd") as sign_date,
 row_number()over(partition by t1.deal_id order by t1.sign_datetime desc) as rn
 
 from ods.yw_sign t1
 where t1.status != -1
 )t2
 where  t2.rn=1
 
)t3
on a1.deal_id=t3.deal_id
;
