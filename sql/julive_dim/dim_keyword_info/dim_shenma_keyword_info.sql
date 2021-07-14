set hive.execution.engine=spark;
set spark.app.name=dim_shenma_keyword_info;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;
drop table if exists tmp_dev_1.tmp_dim_shenma_keyword_info;
  create table tmp_dev_1.tmp_dim_shenma_keyword_info as
  SELECT
      *
  FROM julive_dim.dim_shenma_keyword_info
  WHERE pdate = from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp('${DATE_ID}','yyyyMMdd')),-1),'yyyy-MM-dd'),'yyyyMMdd');


INSERT OVERWRITE TABLE julive_dim.dim_shenma_keyword_info partition(pdate = '${DATE_ID}')
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
regexp_replace(regexp_replace(regexp_extract(pc_url,"(.*?)\\?",0),'"',''),'\\?','') as pc_short_url,
regexp_replace(regexp_replace(regexp_extract(m_url,"(.*?)\\?",0),'"',''),'\\?','') as m_short_url,
regexp_extract(tmp.pc_url,"channel_id=([0-9]+)",1) as pc_channel_id,
regexp_extract(tmp.m_url,"channel_id=([0-9]+)",1) as m_channel_id
          from(
SELECT process1.skey as skey,
       process1.create_date as create_date,
       cast(process1.media_id AS int) AS media_id ,
       cast(process1.module_id AS int) AS module_id ,
       process1.module_name as module_name,
       cast(process1.account_id AS int) AS account_id ,
       cast(process1.plan_id AS bigint) AS plan_id ,
       cast(process1.unit_id AS bigint) AS unit_id ,
       cast(process1.keyword_id AS bigint) AS keyword_id ,
       process1.keyword_name as keyword_name,
       cast(process1.price AS DECIMAL(15,4)) AS price ,
       cast(if(process1.price_old = process1.price,NULL,process1.price_old) AS DECIMAL(15,4)) AS price_old ,
       cast(process1.match_type AS int) AS match_type ,
       cast(if(process1.match_type_old = process1.match_type,NULL,process1.match_type_old) AS int) AS match_type_old ,
       process1.match_type_desc as match_type_desc,
       cast(process1.status AS int) AS status ,
       cast(if(process1.status_old = process1.status,NULL,process1.status_old) AS int) AS status_old ,
       process1.status_desc as status_desc,
       cast(process1.pause AS int) AS pause ,
       cast(if(process1.pause_old = process1.pause,NULL,process1.pause_old) AS int) AS pause_old ,
       process1.pc_url as pc_url,
       process1.m_url as m_url,
       process1.wmatchprefer as wmatchprefer,
       if(process1.wmatchprefer_old = process1.wmatchprefer,NULL,process1.wmatchprefer_old) AS wmatchprefer_old ,
       process1.pc_quality as pc_quality,
       process1.pc_reliable as pc_reliable,
       process1.pc_reason as pc_reason,
       process1.m_quality as m_quality,
       process1.m_reliable as m_reliable,
       process1.m_reason as m_reason,
       Row_number() over(partition by process1.skey,process1.media_id order by process1.skey) as rn
FROM
  (
SELECT if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.keyword_id,tmp_dim_keyword_info.keyword_id) AS skey ,
          from_unixtime(unix_timestamp('${DATE_ID}','yyyyMMdd'),'yyyy-MM-dd') AS create_date ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.media_id,tmp_dim_keyword_info.media_id) AS media_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.module_id,tmp_dim_keyword_info.module_id) AS module_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.module_name,tmp_dim_keyword_info.module_name) AS module_name ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.account_id,tmp_dim_keyword_info.account_id) AS account_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.plan_id,tmp_dim_keyword_info.plan_id) AS plan_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.unit_id,tmp_dim_keyword_info.unit_id) AS unit_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.keyword_id,tmp_dim_keyword_info.keyword_id) AS keyword_id ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.keyword_name,tmp_dim_keyword_info.keyword_name) AS keyword_name ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.price,NULL) AS price ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL
             AND tmp_dim_keyword_info.keyword_id IS NOT NULL,tmp_dim_keyword_info.price,NULL) AS price_old ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.match_type,NULL) AS match_type ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL
             AND tmp_dim_keyword_info.keyword_id IS NOT NULL,tmp_dim_keyword_info.match_type,NULL) AS match_type_old ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.match_type_desc,NULL) AS match_type_desc ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.status,NULL) AS status ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL
             AND tmp_dim_keyword_info.keyword_id IS NOT NULL,tmp_dim_keyword_info.status,NULL) AS status_old ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.status_desc,NULL) AS status_desc ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.pause,NULL) AS pause ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL
             AND tmp_dim_keyword_info.keyword_id IS NOT NULL,tmp_dim_keyword_info.pause,NULL) AS pause_old ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.pc_url,NULL) AS pc_url ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.m_url,NULL) AS m_url ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.wmatchprefer,NULL) AS wmatchprefer ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL
             AND tmp_dim_keyword_info.keyword_id IS NOT NULL,tmp_dim_keyword_info.wmatchprefer,NULL) AS wmatchprefer_old ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.pc_quality,NULL) AS pc_quality ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.pc_reliable,NULL) AS pc_reliable ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.pc_reason,NULL) AS pc_reason ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.m_quality,NULL) AS m_quality ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.m_reliable,NULL) AS m_reliable ,
          if(src_keyword_baseinfo.keyword_id IS NOT NULL,src_keyword_baseinfo.m_reason,NULL) AS m_reason
   FROM
     (SELECT media_id ,
             module_id ,
             module_name ,
             account_id ,
             plan_id ,
             unit_id ,
             keyword_id ,
             keyword_name ,
             price ,
             match_type ,
             status ,
             wmatchprefer ,
             pause
      FROM tmp_dev_1.tmp_dim_shenma_keyword_info)tmp_dim_keyword_info
   FULL JOIN
     (SELECT pdate AS create_date ,
             regexp_replace(split(file_name,'_')[1],'media','') AS media_id ,
             '4' AS module_id ,
             split(file_name,'_')[3] AS module_name ,
             regexp_replace(split(file_name,'_')[2],'accountid','') AS account_id ,
             plan_id AS plan_id ,
             unit_id AS unit_id ,
             keyword_id AS keyword_id ,
             keyword_name AS keyword_name ,
             price AS price ,
             if(match_type = '0','10', 
               if(match_type = '2','30', 
                 if(match_type = '1','23', 
                   if(match_type = '9','22', 
                     if(match_type = '8','21',NULL))))) AS match_type ,
             if(match_type = '0','10-精确匹配',
               if(match_type = '2','30-广泛匹配', 
                 if(match_type = '1','23-短语核心包含', 
                   if(match_type = '9','22-短语精确包含', 
                     if(match_type = '8','21-短语同义包含',NULL))))) AS match_type_desc ,
             if(status = '0','42',
               if(status = '1','46',
                 if(status = '2','43',
                   if(status = '7','41',NULL)))) AS status ,
             if(status = '40','有效-移动 url 审核中', 
               if(status = '7','有效', 
                 if(status = '0','暂停推广', 
                   if(status = '2','不宜推广', 
                     if(status = '44','搜索无效', 
                       if(status = '45','待激活', 
                        if(status = '1','审核中',
                          if(status = '47','搜索量过低', 
                            if(status = '48','部分无效', 
                              if(status = '49','计算机搜索无效',
                                if(status = '50','移动搜索无效',NULL))))))))))) AS status_desc ,
             if(pause = 'true',1,
               if(pause = 'false',0,NULL)) AS pause ,
             null AS pc_url ,
             m_url AS m_url ,
             null AS wmatchprefer ,
             null AS pc_quality ,
             null AS pc_reliable ,
             null AS pc_reason ,
             m_quality AS m_quality ,
             null AS m_reliable ,
             null AS m_reason
      FROM julive_ods.src_shenma_keyword_baseinfo
      WHERE pdate = '${DATE_ID}' )src_keyword_baseinfo 
   ON tmp_dim_keyword_info.keyword_id = src_keyword_baseinfo.keyword_id
    )process1
)tmp

where tmp.rn=1 and tmp.skey is not null;


alter table julive_dim.dim_shenma_keyword_info drop partition (pdate<'${DATE_ID}');
