set hive.execution.engine=spark;
set spark.app.name=app_market_meiti_channel_zhuanhua;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：商务渠道转化统计
--   输 入 表：
--         julive_fact.fact_clue_full_line_indi                        -- 订单宽表
--         julive_fact.fact_event_dtl                                  -- 埋点表
--         julive_dim.dim_city                                         -- 城市维度表
--         julive_dim.dim_channel_info                                 -- 渠道维度表
--         julive_fact.fact_subscribe_dtl                              -- 认购明细


--   输 出 表：julive_app.app_market_meiti_channel_zhuanhua_time
--               
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/06/07 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：

with order as
(
  select 
  create_date,
  distribute_date,
  first_see_date,
  channel_id,
  city_id,
  city_name,
  clue_id,
  distribute_id_list,
  clue_see_list
  from julive_fact.fact_clue_full_line_indi
  where module_id = 15
  and (REGEXP_REPLACE(create_date,'-','') >= '${three_day}' or REGEXP_REPLACE(distribute_date,'-','') >= '${three_day}' or REGEXP_REPLACE(first_see_date,'-','') >= '${three_day}' )
) 

INSERT OVERWRITE TABLE julive_app.app_market_meiti_channel_zhuanhua_time
SELECT  pdate
,channel_id
,channel_name
,channel_type_name
,select_city
,city_name
,sum(view_pv) view_pv
,sum(view_uv) view_uv
,sum(400_pv) 400_pv
,sum(400_uv) 400_uv
,sum(xs_cnt) xs_cnt
,sum(sh_cnt) sh_cnt
,sum(dk_cnt) dk_cnt
,sum(rg_cnt_contains_cancel) rg_cnt_contains_cancel
,sum(rg_cnt_notcontains_cancel) rg_cnt_notcontains_cancel
,sum(income_contains_cancel) income_contains_cancel
,sum(income_notcontains_cancel) income_notcontains_cancel
FROM    (
  SELECT  COALESCE(
    k1.pdate
    ,t1.create_date
    ,t2.distribute_date
    ,t3.first_see_date
    ,t4.first_subscribe_date
  ) pdate
  ,COALESCE(
    k1.channel_id
    ,t1.channel_id
    ,t2.channel_id
    ,t3.channel_id
    ,t4.channel_id
  ) channel_id
  ,t6.channel_name
  ,t6.media_name as channel_type_name
  ,COALESCE(k1.select_city,t1.city_id,t2.city_id,t3.city_id,t4.city_id) select_city
  ,t5.city_name
  ,k1.view_pv
  ,k1.view_uv
  ,k1.400_pv
  ,k1.400_uv
  ,COALESCE(t1.xs_cnt,0) AS xs_cnt
  ,COALESCE(t2.sh_cnt,0) AS sh_cnt
  ,COALESCE(t3.dk_cnt,0) AS dk_cnt
  ,COALESCE(t4.rg_cnt_contains_cancel,0) AS rg_cnt_contains_cancel
  ,COALESCE(t4.rg_cnt_notcontains_cancel,0) AS rg_cnt_notcontains_cancel
  ,COALESCE(t4.income_contains_cancel,0) AS income_contains_cancel
  ,COALESCE(t4.income_notcontains_cancel,0) AS income_notcontains_cancel
  FROM    (
    SELECT  
    pdate
    ,fact_event_dtl.channel_id as channel_id
    ,select_city
    ,view_pv
    ,view_uv
    ,400_pv
    ,400_uv                          
    FROM    
    (
      SELECT  concat_ws(
        '-'
        ,substr(pdate,1,4)
        ,substr(pdate,5,2)
        ,substr(pdate,7,2)
      ) AS pdate
      ,channel_id
      ,select_city
      ,COUNT(IF(event='e_page_view',global_id,NULL)) AS view_pv
      ,COUNT(DISTINCT IF(event='e_page_view',global_id,NULL)) AS view_uv
      ,COUNT(IF(event='e_click_dial_service_call',global_id,NULL)) AS 400_pv
      ,COUNT(DISTINCT IF(event='e_click_dial_service_call',global_id,NULL)) AS 400_uv
      FROM julive_fact.fact_event_dtl
      WHERE   pdate >= '${three_day}'
      AND     event IN ('e_page_view','e_click_dial_service_call') 
      GROUP BY concat_ws('-',substr(pdate,1,4),substr(pdate,5,2),substr(pdate,7,2))
      ,channel_id
      ,select_city
    ) fact_event_dtl
    join 
    (
      select 
      channel_id
      from julive_dim.dim_channel_info
      where module_id = 15
    )dim_channel_info on fact_event_dtl.channel_id = dim_channel_info.channel_id
  ) AS k1
  FULL JOIN (
    SELECT  create_date
    ,channel_id
    ,city_id
    ,city_name
    ,COUNT(DISTINCT clue_id) AS xs_cnt
    FROM    order
    WHERE   REGEXP_REPLACE(create_date,'-','') >= '${three_day}'
    GROUP BY create_date
    ,channel_id
    ,city_id
    ,city_name
  ) t1
  ON      k1.pdate = t1.create_date
  AND     k1.channel_id = t1.channel_id
  AND     k1.select_city = t1.city_id FULL 
  JOIN    (
    SELECT  distribute_date
    ,channel_id
    ,city_id
    ,city_name
    ,COUNT(DISTINCT IF(distribute_id_list IS NOT NULL,clue_id,NULL)) AS sh_cnt
    FROM    order
    WHERE   REGEXP_REPLACE(distribute_date,'-','') >= '${three_day}'
    GROUP BY distribute_date
    ,channel_id
    ,city_id
    ,city_name
  ) AS t2
  ON      k1.pdate = t2.distribute_date
  AND     k1.channel_id = t2.channel_id
  AND     k1.select_city = t2.city_id
  FULL JOIN (
    SELECT  first_see_date
    ,channel_id
    ,city_id
    ,city_name
    ,COUNT(DISTINCT IF(clue_see_list IS NOT NULL,clue_id,NULL)) AS dk_cnt
    FROM    order
    WHERE   REGEXP_REPLACE(first_see_date,'-','') >= '${three_day}'
    GROUP BY first_see_date
    ,channel_id
    ,city_id
    ,city_name
  ) AS t3
  ON      k1.pdate = t3.first_see_date
  AND     k1.channel_id = t3.channel_id
  AND     k1.select_city = t3.city_id FULL
  JOIN    (
    SELECT  TO_DATE(subscribe_time) first_subscribe_date
    ,channel_id
    ,city_id
    ,city_name
    ,COUNT(DISTINCT subscribe_id) rg_cnt_contains_cancel
    ,sum(subscribe_contains_cancel_ext_num) AS rg_cnt_contains_cancel_bat
    ,sum(subscribe_contains_ext_num) AS rg_cnt_notcontains_cancel
    ,sum(subscribe_contains_cancel_ext_income) AS income_contains_cancel
    ,sum(subscribe_contains_ext_income) AS income_notcontains_cancel
    FROM    julive_fact.fact_subscribe_dtl
    WHERE   REGEXP_REPLACE(TO_DATE(subscribe_time),'-','') >= '${three_day}'
    AND     channel_id IN ('20383693','20418105','20424867','20466426','20466425','20466424','20484605','20484604','20511510','20511508','20511356','20501859','20508159','20514869','20511507','20519716','20520756','20531990','20531984','20531983','20537498','20537497','20537496','20537495','20537494','20537493','20537492','20537491','20548303','20548302','20548301','20548300','20548305','20548304','20561478','20561397','20561396','20561395','20561394','20561378','20561379' ,'20561380','20561381','20561382','20561383')
    GROUP BY TO_DATE(subscribe_time)
    ,channel_id
    ,city_id
    ,city_name
  ) AS t4
  ON      k1.pdate = t4.first_subscribe_date
  AND     k1.channel_id = t4.channel_id
  AND     k1.select_city = t4.city_id
  LEFT JOIN julive_dim.dim_city AS t5
  ON      COALESCE(k1.select_city,t1.city_id,t2.city_id,t3.city_id,t4.city_id) = t5.city_id LEFT
  JOIN    julive_dim.dim_channel_info AS t6
  ON      COALESCE(k1.channel_id,t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id) = t6.channel_id
) aaa
GROUP BY pdate
,channel_id
,channel_name
,channel_type_name
,select_city
,city_name
UNION ALL
SELECT  *
FROM    julive_app.app_market_meiti_channel_zhuanhua_time
WHERE   REGEXP_REPLACE(pdate,'-','') < '${three_day}'
;


DROP VIEW IF EXISTS tmp_cp.meiti_channel_zhuanhua_dim_time;
CREATE VIEW IF NOT EXISTS tmp_cp.meiti_channel_zhuanhua_dim_time (
pdate                                                COMMENT '事件日期',   
channel_id                                           COMMENT '渠道ID',   
channel_name                                         COMMENT '渠道名称',   
channel_type_name                                    COMMENT '渠道分类名称',  
select_city                                          COMMENT '城市ID',
city_name                                            COMMENT '城市名称',
view_pv                                              COMMENT 'PV',
view_uv                                              COMMENT 'UV',   
400_pv                                               COMMENT '400PV', 
400_uv                                               COMMENT '400UV',
xs_cnt                                               COMMENT '线索数',    
sh_cnt                                               COMMENT '上户数',
dk_cnt                                               COMMENT '带看数',
rg_cnt_contains_cancel                               COMMENT '认购数含退',
rg_cnt_notcontains_cancel                            COMMENT '认购数不含退',
income_contains_cancel                               COMMENT '认购金额含退',
income_notcontains_cancel                            COMMENT '认购金额不含退')
COMMENT '商务渠道转化指标时间维度'
AS SELECT * FROM julive_app.app_market_meiti_channel_zhuanhua_time;

