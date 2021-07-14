set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=fact_comjiaunique_final_version_dtl;
set mapred.job.name=fact_comjiaunique_final_version_dtl;
set spark.yarn.queue=etl;
insert overwrite table tmp_dev_1.tmp_event_global_version partition(pdate=${hiveconf:etl_date})

select

global_id,
comjia_unique_id,
product_id,
select_city,
get_json_object(properties,'$.manufacturer')                    as manufacturer,
get_json_object(properties,'$.app_version')                     as app_version,
get_json_object(regexp_replace(properties,'\\$','p_'),'$.p_os') as os_system,
get_json_object(properties,'$.os_version')                      as os_version,
create_timestamp 

from julive_fact.fact_event_dtl

where pdate = ${hiveconf:etl_date}

;
