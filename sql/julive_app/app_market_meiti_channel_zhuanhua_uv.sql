set hive.execution.engine=spark;
set spark.app.name=app_market_meiti_channel_zhuanhua_uv;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：APP创意层级展点消以及业务行为明细
--   输 入 表：
--         julive_fact.fact_event_dtl                                 -- 埋点表
--         julive_fact.global_id_hbase_2_hive                         -- 唯一ID
--         julive_fact.fact_clue_full_line_indi                       -- 业务行为总表
--         ods.cj_channel                                             -- 渠道表
--         julive_dim.dim_city                                        -- 城市维度



--   输 出 表：julive_app.app_market_meiti_channel_zhuanhua_uv
--               
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/06/08 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：

INSERT INTO TABLE julive_app.app_market_meiti_channel_zhuanhua_uv
SELECT  ALL.pdate
        ,ALL.channel_id
        ,ALL.select_city
        ,ALL.view_pv
        ,ALL.view_uv
        ,ALL.pv_400
        ,ALL.uv_400
        ,COALESCE(ALL.xs_cnt,0) AS xs_cnt
        ,COALESCE(ALL.sh_cnt,0) AS sh_cnt
        ,COALESCE(ALL.dk_cnt,0) AS dk_cnt
        ,COALESCE(ALL.rg_cnt_contains_cancel,0) AS rg_cnt_contains_cancel
        ,COALESCE(ALL.rg_cnt_notcontains_cancel,0) AS rg_cnt_notcontains_cancel
        ,COALESCE(ALL.income_contains_cancel,0) AS income_contains_cancel
        ,COALESCE(ALL.income_notcontains_cancel,0) AS income_notcontains_cancel
        ,t6.channel_name
        ,t6.channel_type_name
        ,t5.city_name
FROM    (
            SELECT  t.pdate
                    ,t.channel_id
                    ,t.select_city
                    ,COUNT(IF(event='e_page_view',global_id,NULL)) AS view_pv
                    ,COUNT(DISTINCT IF(event='e_page_view',global_id,NULL)) AS view_uv
                    ,COUNT(IF(event='e_click_dial_service_call',global_id,NULL)) AS pv_400
                    ,COUNT(DISTINCT IF(event='e_click_dial_service_call',global_id,NULL)) AS uv_400
                    ,COUNT(DISTINCT clue_id) AS xs_cnt
                    ,COUNT(DISTINCT IF(distribute_id_list IS NOT NULL,clue_id,NULL)) AS sh_cnt
                    ,COUNT(DISTINCT IF(clue_see_list IS NOT NULL,clue_id,NULL)) AS dk_cnt
                    ,sum(
                        IF(subscribe_contains_cancel_ext_id_list IS NOT NULL,subscribe_num,0)
                    ) AS rg_cnt_contains_cancel
                    ,sum(
                        IF(clue_subscribe_list IS NOT NULL,subscribe_contains_ext_num,0)
                    ) AS rg_cnt_notcontains_cancel
                    ,sum(
                        IF(
                            subscribe_contains_cancel_ext_id_list IS NOT NULL
                            ,subscribe_contains_cancel_ext_income
                            ,0
                        )
                    ) AS income_contains_cancel
                    ,sum(
                        IF(clue_subscribe_list IS NOT NULL,subscribe_contains_ext_income,0)
                    ) AS income_notcontains_cancel
            FROM    (
                        SELECT  pdate
                                ,channel_id
                                ,select_city
                                ,global_id
                                ,event
                                ,julive_id
                                ,clue_id
                                ,distribute_id_list
                                ,clue_see_list
                                ,clue_subscribe_list
                                ,subscribe_contains_cancel_ext_id_list
                                ,subscribe_contains_cancel_ext_income
                                ,subscribe_contains_ext_income
                                ,subscribe_contains_ext_num
                                ,subscribe_num
                        FROM    (
                                    SELECT  pdate
                                            ,channel_id
                                            ,select_city
                                            ,k1.global_id
                                            ,event
                                            ,julive_id
                                    FROM    (
                                                SELECT  concat_ws(
                                                            '-'
                                                            ,substr(pdate,1,4)
                                                            ,substr(pdate,5,2)
                                                            ,substr(pdate,7,2)
                                                        ) AS pdate
                                                        ,channel_id
                                                        ,select_city
                                                        ,global_id
                                                        ,event
                                                FROM    julive_fact.fact_event_dtl
                                                WHERE   pdate = '${last_day}'
                                                AND     event IN ('e_page_view','e_click_dial_service_call')
                                                AND     channel_id = '158'
                                                AND     get_json_object(regexp_replace(properties,"\\$","p_"),"$.p_latest_referrer") IN ('https://fangchan.hao.360.cn/','https://hao.360.cn/')
                                                GROUP BY concat_ws('-',substr(pdate,1,4),substr(pdate,5,2),substr(pdate,7,2))
                                                         ,channel_id
                                                         ,select_city
                                                         ,global_id
                                                         ,event
                                            ) AS k1
                                    LEFT JOIN (
                                                  SELECT  DISTINCT global_id
                                                          ,tt.bb AS julive_id
                                                  FROM    julive_fact.global_id_hbase_2_hive
                                                  LATERAL VIEW EXPLODE(SPLIT(julive_id,'\\|')) tt AS bb
                                              ) oim
                                    ON      CAST(k1.global_id AS STRING) = oim.global_id
                                ) AS online
                        LEFT JOIN (
                                      SELECT  user_id
                                              ,clue_id
                                              ,distribute_id_list
                                              ,clue_see_list
                                              ,clue_subscribe_list
                                              ,subscribe_contains_cancel_ext_id_list
                                              ,subscribe_contains_cancel_ext_income
                                              ,subscribe_contains_ext_income
                                              ,subscribe_contains_ext_num
                                              ,subscribe_num
                                      FROM    julive_fact.fact_clue_full_line_indi
                                      WHERE   REGEXP_REPLACE(create_date,'-','') = '${last_day}'
                                  ) AS t4
                        ON      online.julive_id = t4.user_id
                    ) AS t
            GROUP BY t.pdate
                     ,t.channel_id
                     ,t.select_city
        ) AS ALL
LEFT JOIN julive_dim.dim_city AS t5
ON      ALL.select_city = t5.city_id LEFT
JOIN    ods.cj_channel AS t6
ON      ALL.channel_id = t6.channel_id
GROUP BY ALL.pdate
         ,ALL.channel_id
         ,ALL.select_city
         ,ALL.view_pv
         ,ALL.view_uv
         ,ALL.pv_400
         ,ALL.uv_400
         ,COALESCE(ALL.xs_cnt,0)
         ,COALESCE(ALL.sh_cnt,0)
         ,COALESCE(ALL.dk_cnt,0)
         ,COALESCE(ALL.rg_cnt_contains_cancel,0)
         ,COALESCE(ALL.rg_cnt_notcontains_cancel,0)
         ,COALESCE(ALL.income_contains_cancel,0)
         ,COALESCE(ALL.income_notcontains_cancel,0)
         ,t6.channel_name
         ,t6.channel_type_name
         ,t5.city_name
;


DROP VIEW IF EXISTS tmp_cp.meiti_channel_zhuanhua_dim_uv;
CREATE VIEW IF NOT EXISTS tmp_cp.meiti_channel_zhuanhua_dim_uv (
pdate                                                COMMENT '事件日期',   
channel_id                                           COMMENT '渠道ID',       
select_city                                          COMMENT '城市ID',
view_pv                                              COMMENT 'PV',
view_uv                                              COMMENT 'UV',   
pv_400                                               COMMENT '400PV', 
uv_400                                               COMMENT '400UV',
xs_cnt                                               COMMENT '线索数',    
sh_cnt                                               COMMENT '上户数',
dk_cnt                                               COMMENT '带看数',
rg_cnt_contains_cancel                               COMMENT '认购数含退',
rg_cnt_notcontains_cancel                            COMMENT '认购数不含退',
income_contains_cancel                               COMMENT '认购金额含退',
income_notcontains_cancel                            COMMENT '认购金额不含退',
channel_name                                         COMMENT '渠道名称',   
channel_type_name                                    COMMENT '渠道分类名称',  
city_name                                            COMMENT '城市名称')
COMMENT '商务渠道转化指标UV'
AS SELECT * FROM julive_app.app_market_meiti_channel_zhuanhua_uv;





