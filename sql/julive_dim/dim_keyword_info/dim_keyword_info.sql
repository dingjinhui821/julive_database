
set hive.execution.engine=spark;
set spark.app.name=dim_keyword_info;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE julive_dim.dim_keyword_info partition(pdate = '${DATE_ID}',account)

select 
tmp.skey,            
tmp.create_date,
tmp.media_id,        
tmp.module_id,       
tmp.module_name,     
tmp.account_id,      
tmp.plan_id,      
tmp.unit_id,         
tmp.keyword_id,      
tmp.keyword_name,    
tmp.price,    
tmp.price_old,       
tmp.match_type,      
tmp.match_type_old,  
tmp.match_type_desc, 
tmp.status, 
tmp.status_old,      
tmp.status_desc,     
tmp.pause,     
tmp.pause_old,       
tmp.pc_url,          
tmp.m_url,       
tmp.wmatchprefer,    
tmp.wmatchprefer_old,
tmp.pc_quality,      
tmp.pc_reliable,    
tmp.pc_reason,       
tmp.m_quality,       
tmp.m_reliable,      
tmp.m_reason,
if (tmp.pc_short_url is not null and tmp.pc_short_url !='' and tmp.pc_short_url !='NULL' ,tmp.pc_short_url,'') as pc_short_url,
if (tmp.m_short_url is not null and tmp.m_short_url !='' and tmp.m_short_url !='NULL' ,tmp.m_short_url,'') as m_short_url,
tmp.pc_channel_id,
tmp.m_channel_id,
current_timestamp() as etl_time,
tmp.account_id%10       as account
from 
(
select 
skey,            
create_date,
media_id,        
module_id,       
module_name,     
account_id,      
plan_id,      
unit_id,         
keyword_id,      
keyword_name,    
price,    
price_old,       
match_type,      
match_type_old,  
match_type_desc, 
status, 
status_old,      
status_desc,     
pause,     
pause_old,       
pc_url,          
m_url,       
wmatchprefer,    
wmatchprefer_old,
pc_quality,      
pc_reliable,    
pc_reason,       
m_quality,       
m_reliable,      
m_reason,
if (pc_short_url is not null and pc_short_url !=''and pc_short_url !='NULL' ,pc_short_url,pc_url) as pc_short_url,
if (m_short_url is not null and m_short_url !='' and m_short_url!='NULL' ,m_short_url,m_url) as m_short_url,
if(pc_channel_id is not null and pc_channel_id !='' and pc_channel_id !='NULL',pc_channel_id,'0') as pc_channel_id,
if(m_channel_id is not null and m_channel_id !='' and m_channel_id !='NULL',m_channel_id,'0') as m_channel_id

 from 
julive_dim.dim_baidu_keyword_info where pdate = '${DATE_ID}' and skey is not null) tmp;


INSERT into TABLE julive_dim.dim_keyword_info partition(pdate = '${DATE_ID}',account)
select 
tmp.skey,            
tmp.create_date,
tmp.media_id,        
tmp.module_id,       
tmp.module_name,     
tmp.account_id,      
tmp.plan_id,      
tmp.unit_id,         
tmp.keyword_id,      
tmp.keyword_name,    
tmp.price,    
tmp.price_old,       
tmp.match_type,      
tmp.match_type_old,  
tmp.match_type_desc, 
tmp.status, 
tmp.status_old,      
tmp.status_desc,     
tmp.pause,     
tmp.pause_old,       
tmp.pc_url,          
tmp.m_url,       
tmp.wmatchprefer,    
tmp.wmatchprefer_old,
tmp.pc_quality,      
tmp.pc_reliable,    
tmp.pc_reason,       
tmp.m_quality,       
tmp.m_reliable,      
tmp.m_reason,
if (tmp.pc_short_url is not null and tmp.pc_short_url !='' and tmp.pc_short_url !='NULL' ,tmp.pc_short_url,'') as pc_short_url,
if (tmp.m_short_url is not null and tmp.m_short_url !='' and tmp.m_short_url !='NULL' ,tmp.m_short_url,'') as m_short_url,
tmp.pc_channel_id,
tmp.m_channel_id,
current_timestamp() as etl_time,
tmp.account_id%10       as account 
from
(
select 
skey,            
create_date,
media_id,        
module_id,       
module_name,     
account_id,      
plan_id,      
unit_id,         
keyword_id,      
keyword_name,    
price,    
price_old,       
match_type,      
match_type_old,  
match_type_desc, 
status, 
status_old,      
status_desc,     
pause,     
pause_old,       
pc_url,          
m_url,       
wmatchprefer,    
wmatchprefer_old,
pc_quality,      
pc_reliable,    
pc_reason,       
m_quality,       
m_reliable,      
m_reason,
if (pc_short_url is not null and pc_short_url !=''and pc_short_url !='NULL' ,pc_short_url,pc_url) as pc_short_url,
if (m_short_url is not null and m_short_url !='' and m_short_url !='NULL' ,m_short_url,m_url) as m_short_url,
if(pc_channel_id is not null and pc_channel_id !='' and pc_channel_id !='NULL',pc_channel_id,'0') as pc_channel_id,
if(m_channel_id is not null and m_channel_id !='' and m_channel_id !='NULL',m_channel_id,'0') as m_channel_id

 from 
julive_dim.dim_shenma_keyword_info where pdate = '${DATE_ID}' and skey is not null)tmp;


INSERT into TABLE julive_dim.dim_keyword_info partition(pdate = '${DATE_ID}',account)
select 
tmp.skey,            
tmp.create_date,
tmp.media_id,        
tmp.module_id,       
tmp.module_name,     
tmp.account_id,      
tmp.plan_id,      
tmp.unit_id,         
tmp.keyword_id,      
tmp.keyword_name,    
tmp.price,    
tmp.price_old,       
tmp.match_type,      
tmp.match_type_old,  
tmp.match_type_desc, 
tmp.status, 
tmp.status_old,      
tmp.status_desc,     
tmp.pause,     
tmp.pause_old,       
tmp.pc_url,          
tmp.m_url,       
tmp.wmatchprefer,    
tmp.wmatchprefer_old,
tmp.pc_quality,      
tmp.pc_reliable,    
tmp.pc_reason,       
tmp.m_quality,       
tmp.m_reliable,      
tmp.m_reason,
if (tmp.pc_short_url is not null and tmp.pc_short_url !='' and tmp.pc_short_url !='NULL' ,tmp.pc_short_url,'') as pc_short_url,
if (tmp.m_short_url is not null and tmp.m_short_url !='' and tmp.m_short_url !='NULL' ,tmp.m_short_url,'') as m_short_url,
tmp.pc_channel_id,
tmp.m_channel_id,
current_timestamp() as etl_time,
tmp.account_id%10       as account 
from
(
select 
skey,            
create_date,
media_id,        
module_id,       
module_name,     
account_id,      
plan_id,      
unit_id,         
keyword_id,      
keyword_name,    
price,    
price_old,       
match_type,      
match_type_old,  
match_type_desc, 
status, 
status_old,      
status_desc,     
pause,     
pause_old,       
pc_url,          
m_url,       
wmatchprefer,    
wmatchprefer_old,
pc_quality,      
pc_reliable,    
pc_reason,       
m_quality,       
m_reliable,      
m_reason,
if (pc_short_url is not null and pc_short_url !=''and pc_short_url !='NULL' ,pc_short_url,pc_url) as pc_short_url,
if (m_short_url is not null and m_short_url !='' and m_short_url !='NULL' ,m_short_url,m_url) as m_short_url,
if(pc_channel_id is not null and pc_channel_id !='' and pc_channel_id !='NULL',pc_channel_id,'0') as pc_channel_id,
if(m_channel_id is not null and m_channel_id !='' and m_channel_id !='NULL',m_channel_id,'0') as m_channel_id
 from 
julive_dim.dim_sougou_keyword_info where pdate = '${DATE_ID}' and skey is not null)tmp;


INSERT into TABLE julive_dim.dim_keyword_info partition(pdate = '${DATE_ID}',account)
select 
tmp.skey,            
tmp.create_date,
tmp.media_id,        
tmp.module_id,       
tmp.module_name,     
tmp.account_id,      
tmp.plan_id,      
tmp.unit_id,         
tmp.keyword_id,      
tmp.keyword_name,    
tmp.price,    
tmp.price_old,       
tmp.match_type,      
tmp.match_type_old,  
tmp.match_type_desc, 
tmp.status, 
tmp.status_old,      
tmp.status_desc,     
tmp.pause,     
tmp.pause_old,       
tmp.pc_url,          
tmp.m_url,       
tmp.wmatchprefer,    
tmp.wmatchprefer_old,
tmp.pc_quality,      
tmp.pc_reliable,    
tmp.pc_reason,       
tmp.m_quality,       
tmp.m_reliable,      
tmp.m_reason,
if (tmp.pc_short_url is not null and tmp.pc_short_url !='' and tmp.pc_short_url !='NULL' ,tmp.pc_short_url,'') as pc_short_url,
if (tmp.m_short_url is not null and tmp.m_short_url !='' and tmp.m_short_url !='NULL' ,tmp.m_short_url,'') as m_short_url,
tmp.pc_channel_id,
tmp.m_channel_id,
current_timestamp() as etl_time,
tmp.account_id%10       as account 
from
(
select 
skey,            
create_date,
media_id,        
module_id,       
module_name,     
account_id,      
plan_id,      
unit_id,         
keyword_id,      
keyword_name,    
price,    
price_old,       
if(match_type = '10','10',
  if(match_type = '25','25',
  if(match_type = '30','30',
  if(match_type = '20','20',NULL)))) AS match_type ,
if(match_type = '10','10',
  if(match_type = '25','25',
  if(match_type = '30','30', 
  if(match_type = '20','20', NULL)))) AS match_type_old , 
if(match_type_desc = '10-精确匹配','10-精确匹配',
  if(match_type_desc = '22-智能短语匹配','22-智能短语匹配',
  if(match_type_desc = '30-广泛匹配','30-广泛匹配', 
  if(match_type_desc = '21-普通短语匹配','21-普通短语匹配', NULL)))) AS match_type_desc ,
status, 
status_old,      
status_desc,     
pause,     
pause_old,       
pc_url,          
m_url,       
wmatchprefer,    
wmatchprefer_old,
pc_quality,      
pc_reliable,    
pc_reason,       
m_quality,       
m_reliable,      
m_reason,
if (pc_short_url is not null and pc_short_url !=''and pc_short_url !='NULL' ,pc_short_url,pc_url) as pc_short_url,
if (m_short_url is not null and m_short_url !='' and m_short_url !='NULL' ,m_short_url,m_url) as m_short_url,
if(pc_channel_id is not null and pc_channel_id !='' and pc_channel_id !='NULL',pc_channel_id,'0') as pc_channel_id,
if(m_channel_id is not null and m_channel_id !='' and m_channel_id !='NULL',m_channel_id,'0') as m_channel_id
 from 
julive_dim.dim_qihoo_keyword_info where pdate = '${DATE_ID}' and skey is not null)tmp;

alter table julive_dim.dim_keyword_info        drop partition (pdate<'${DATE_ID}');
