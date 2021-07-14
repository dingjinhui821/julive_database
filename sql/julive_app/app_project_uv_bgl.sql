
-- drop table if exists julive_app.app_project_uv_bgl;
-- CREATE TABLE julive_app.app_project_uv_bgl( 
-- pdate  string COMMENT '创建时间',
-- time_p  string COMMENT '浏览时间',
-- project_id bigint COMMENT '楼盘id',
-- product_id  bigint COMMENT '设备id',
-- city_id  bigint COMMENT '城市id',
-- name   string COMMENT '楼盘名称',
-- bgl bigint COMMENT '曝光量',
-- uv bigint COMMENT 'UV'
-- ) stored as parquet 
-- ;
set hive.execution.engine=spark;
set spark.yarn.queue=etl;
set spark.app.name=app_project_uv_bgl; 

INSERT overwrite TABLE julive_app.app_project_uv_bgl
SELECT bg.pdate,
       bg.time_p,
       bg.project_id,
       bg.product_id,
       cj_project.city_id,
       cj_project.name,
       bg.bgl,
       lv.uv
FROM
  (SELECT pdate,
          from_unixtime(unix_timestamp(pdate,'yyyymmdd'),'yyyy-mm-dd') time_p,
          project_id,
          (CASE
               WHEN substr(product_id,1,3)=101
                    OR substr(product_id,1,3)=201 THEN 'APP'
               WHEN product_id=1 THEN 'PC'
               WHEN product_id=2 THEN 'M'
               WHEN product_id= IN (301,
                                    401) THEN '小程序'
               ELSE '其它'
           END) AS product_id,
          count(distinct(global_id)) AS bgl
   FROM julive_fact.fact_event_dtl
   WHERE pdate >= '20191201'
     AND project_id > 0
     AND event='e_module_exposure'
   GROUP BY pdate, (CASE
                        WHEN substr(product_id,1,3)=101
                             OR substr(product_id,1,3)=201 THEN 'APP'
                        WHEN product_id=1 THEN 'PC'
                        WHEN product_id=2 THEN 'M'
                        WHEN product_id= IN (301,
                                             401) THEN '小程序'
                        ELSE '其它'
                    END), from_unixtime(unix_timestamp(pdate,'yyyymmdd'),'yyyy-mm-dd'),
                          project_id)bg
LEFT JOIN
  (SELECT pdate,
          from_unixtime(unix_timestamp(pdate,'yyyymmdd'),'yyyy-mm-dd') time_p,
          project_id,
          (CASE
               WHEN substr(product_id,1,3)=101
                    OR substr(product_id,1,3)=201 THEN 'APP'
               WHEN product_id=1 THEN 'PC'
               WHEN product_id=2 THEN 'M'
               WHEN product_id= IN (301,
                                    401) THEN '小程序'
               ELSE '其它'
           END) AS product_id,
          count(distinct(global_id)) AS uv
   FROM julive_fact.fact_event_dtl
   WHERE pdate >= '20191201'
     AND project_id > 0
     AND topage IN ('p_project_details',
                    'p_project_home')
     AND event='e_page_view'
   GROUP BY pdate,
            from_unixtime(unix_timestamp(pdate,'yyyymmdd'),'yyyy-mm-dd'),
            project_id, (CASE
                             WHEN substr(product_id,1,3)=101
                                  OR substr(product_id,1,3)=201 THEN 'APP'
                             WHEN product_id=1 THEN 'PC'
                             WHEN product_id=2 THEN 'M'
                             WHEN product_id= IN (301,
                                                  401) THEN '小程序'
                             ELSE '其它'
                         END))lv ON bg.pdate=lv.pdate
AND bg.project_id=lv.project_id
AND bg.product_id=lv.product_id
LEFT JOIN ods.cj_project ON bg.project_id=cj_project.project_id 
;

