set etl_date = '${hiveconf:etlDate}';
set hive.execution.engine=spark;
set spark.app.name=dwd_unique_julive_id_mapping_all;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=2048; 
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE tmp_dev_1.tmp_dwd_esf_unique_julive_id_mapping_daily
SELECT
DISTINCT comjia_unique_id,
julive_id
FROM julive_fact.fact_event_esf_dtl
WHERE
pdate=${hiveconf:etl_date}
AND comjia_unique_id <> ''
AND julive_id NOT IN ("-1", "-1.0", "-0", "0", "0.0", "-0.0", "")
;

INSERT OVERWRITE TABLE dwd.dwd_esf_unique_julive_id_mapping
SELECT
DISTINCT comjia_unique_id,
julive_id
FROM
(SELECT
comjia_unique_id,
julive_id
FROM dwd.dwd_esf_unique_julive_id_mapping
UNION ALL
SELECT
comjia_unique_id,
julive_id
FROM tmp_dev_1.tmp_dwd_esf_unique_julive_id_mapping_daily) t1
WHERE t1.julive_id NOT IN ("-1", "-1.0", "-0", "0", "0.0", "-0.0", "")
;


INSERT OVERWRITE TABLE dwd.dwd_unique_julive_id_mapping_all
SELECT
DISTINCT comjia_unique_id,
julive_id
FROM
(SELECT
comjia_unique_id,
julive_id
FROM dwd.dwd_unique_julive_id_mapping
UNION ALL
SELECT
comjia_unique_id,
julive_id
FROM dwd.dwd_esf_unique_julive_id_mapping) t1
WHERE t1.julive_id NOT IN ("-1", "-1.0", "-0", "0", "0.0", "-0.0", "")
;
