set hive.execution.engine=spark;
set spark.app.name=dim_wlmq_city;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;



insert overwrite table julive_dim.dim_wlmq_city
select 
tmp2.skey                                    as skey,
tmp2.from_source                             as from_source,
tmp2.city_id                                 as city_id,
tmp2.city_name                               as city_name,
concat(tmp2.skey,'',tmp2.city_name)          as city_seq,
tmp2.region                                  as region, 
tmp2.city_type                               as city_type, 
tmp2.city_type_first                         as city_type_first, 
tmp2.city_type_second                        as city_type_second, 
tmp2.city_type_third                         as city_type_third, 
tmp2.mgr_city_seq                            as mgr_city_seq, 
tmp2.mgr_city                                as mgr_city, 
tmp2.deputy_city                             as deputy_city
from
(
select 
row_number() over(partition by 1 order by 1) as skey,
tmp1.from_source                             as from_source,
tmp1.city_id                                 as city_id,
tmp1.city_name                               as city_name,
tmp1.region                                  as region, 
tmp1.city_type                               as city_type, 
tmp1.city_type_first                         as city_type_first, 
tmp1.city_type_second                        as city_type_second, 
tmp1.city_type_third                         as city_type_third, 
tmp1.mgr_city_seq                            as mgr_city_seq, 
tmp1.mgr_city                                as mgr_city, 
tmp1.deputy_city                             as deputy_city     
from 

(select 

2                                            as from_source,
t1.city_id                                   as city_id,
t2.name_cn                                   as city_name,
''                                           as city_seq,
''                                           as region, 
''                                           as city_type, 
''                                           as city_type_first, 
''                                           as city_type_second, 
''                                           as city_type_third, 
''                                           as mgr_city_seq, 
''                                           as mgr_city, 
''                                           as deputy_city
from ods.yw_developer_city_config t1
left join ods.cj_district t2 on t1.city_id = t2.id

union all

select 
3                                            as from_source,
t1.virtual_city                              as city_id,
t2.name_cn                                   as city_name,
''                                           as city_seq,
''                                           as region, 
''                                           as city_type, 
''                                           as city_type_first, 
''                                           as city_type_second, 
''                                           as city_type_third, 
''                                           as mgr_city_seq, 
''                                           as mgr_city, 
''                                           as deputy_city
from ods.yw_esf_virtual_config t1
left join ods.cj_district t2 on t1.virtual_city = t2.id
   ) tmp1
)tmp2;


