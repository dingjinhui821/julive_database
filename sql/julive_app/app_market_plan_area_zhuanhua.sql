set hive.execution.engine=spark;
set spark.app.name=app_market_plan_area_zhuanhua;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：今日头条带城市数据统计参考表（数据不全）
--   输 入 表：
--         ods.dsp_creative_report           -- 创意展点消
--         ods.dsp_feed_area_plan_stat       -- 今日头条带地域报告
--         tmp_etl.creative_app_jihuo        -- app激活以及业务行为
--         tmp_etl.app_zhuanhua_time         -- 业务行为和认购金额
--         ods.dsp_account_rebate            -- 账号返点
--         julive_dim.dim_dsp_account        -- 账户维度表


--   输 出 表：julive_app.app_market_plan_area_zhuanhua
--                
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/05/25 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

WITH plan AS
(SELECT
        plan_id,
        plan_name
FROM
(SELECT
        unit_id as plan_id,
        unit_name as plan_name,
        row_number() over(PARTITION BY unit_id ORDER BY report_date) as r
from ods.dsp_creative_report
WHERE from_unixtime( report_date,'yyyy-MM-dd' )>='2020-07-09'
AND unit_id != 0
AND unit_id IS NOT NULL
)p
WHERE r = 1
)--由于头条的账户层级与百度不同，所以将计划存成了单元，直接取用单元即可


INSERT OVERWRITE TABLE julive_app.app_market_plan_area_zhuanhua
SELECT
        ppp.*,
        plan.plan_name
FROM
    (SELECT
            report_date,
            account_name,
            plan_id,
            city,
            media_type_name,
            app_type,
            sum(show_num) as show_num,
            sum(click_num) as click_num,
            sum(bill_cost) as bill_cost,
            sum(cost) AS cost,
            sum(jh_cnt) AS jh_cnt,
            sum(xs_cnt) AS xs_cnt,
            sum(sh_cnt) as sh_cnt,
            sum(dk_cnt) as dk_cnt,
            sum(rg_cnt) as rg_cnt,
            sum(xs_cnt_time) AS xs_cnt_time,
            sum(sh_cnt_time) as sh_cnt_time,
            sum(dk_cnt_time) as dk_cnt_time,
            sum(rg_cnt_time) as rg_cnt_time
    FROM
        (SELECT
                coalesce(cost.report_date,zhuanhua.install_date_time,zhuanhua_time.report_date) as report_date,
                cost.account_name,
                coalesce(cost.plan_id,zhuanhua.aid,zhuanhua_time.aid) AS plan_id,
                coalesce(cost.media_type_name,zhuanhua.channel_type_name,zhuanhua_time.media_type) AS media_type_name,
                coalesce(cost.app_type,zhuanhua.app_id,zhuanhua_time.app_id) AS app_type,
                coalesce(cost.city_name,zhuanhua.city,zhuanhua_time.city) AS city,
                cost.show_num,
                cost.click_num,
                cost.bill_cost,
                cost.cost,
                zhuanhua.jh_cnt,
                zhuanhua.xs_cnt,
                zhuanhua.sh_cnt,
                zhuanhua.dk_cnt,
                zhuanhua.rg_cnt,
                zhuanhua_time.xs_cnt_time,
                zhuanhua_time.sh_cnt_time,
                zhuanhua_time.dk_cnt_time,
                zhuanhua_time.rg_cnt_time
        FROM
        (SELECT
            p1.*,
            (p1.bill_cost/(1+dsp_account_rebate.rebate)) as cost
            FROM
            (SELECT
            from_unixtime( cur_date,'yyyy-MM-dd' ) as report_date,
            dsp_feed_area_plan_stat.account_name,
            dsp_feed_area_plan_stat.dsp_account_id ,
            unit_id as plan_id ,
            app_type,
            media_type_name,
            city_name,
            sum(show) as show_num ,
            sum(click) as click_num,
            sum(cost) as bill_cost
            FROM ods.dsp_feed_area_plan_stat
            LEFT JOIN julive_dim.dim_dsp_account
            ON dsp_feed_area_plan_stat.dsp_account_id = dim_dsp_account.id
            WHERE media_type_name = '今日头条'
            AND product_type_name = 'APP渠道'
            AND from_unixtime( cur_date ,'yyyy') >= '2019'
            AND dsp_feed_area_plan_stat.data_type != 2
            GROUP BY
            from_unixtime( cur_date,'yyyy-MM-dd' ),
            dsp_feed_area_plan_stat.account_name,
            dsp_feed_area_plan_stat.dsp_account_id ,
            unit_id ,
            app_type,
            city_name,
            media_type_name
            )p1
            left join
            (select
            dsp_account_id,
            from_unixtime(start_date,'yyyy-MM-dd') as start_date,
            from_unixtime(end_date,'yyyy-MM-dd') as end_date,
            rebate
            from ods.dsp_account_rebate
            where status = 1
            )dsp_account_rebate
            on p1.dsp_account_id=dsp_account_rebate.dsp_account_id
        where p1.report_date BETWEEN dsp_account_rebate.start_date and dsp_account_rebate.end_date
        AND p1.report_date >= '2020-07-09'
        )cost
        
        
        FULL JOIN
        (SELECT
                city,
                to_date(install_date_time) AS install_date_time,
                channel_type_name,
                if(app_id = '101','安卓',if(app_id = '201','苹果',NULL)) as app_id,
                aid,
                count(DISTINCT global_id) AS jh_cnt,
                count(DISTINCT order_id) AS xs_cnt,
                count(DISTINCT sh_order_id) AS sh_cnt,
                count(DISTINCT dk_order_id) AS dk_cnt,
                count(DISTINCT rg_order_id) AS rg_cnt
        FROM
            tmp_etl.creative_app_jihuo
        GROUP BY
                city,
                to_date(install_date_time),
                channel_type_name,
                if(app_id = '101','安卓',if(app_id = '201','苹果',NULL)),
                aid
        HAVING channel_type_name = '今日头条' OR channel_type_name = '百度信息流'
        )zhuanhua
        --uv维度的转化
        ON cost.report_date = zhuanhua.install_date_time
        AND cost.plan_id = zhuanhua.aid
        AND cost.media_type_name = zhuanhua.channel_type_name
        AND cost.app_type = zhuanhua.app_id
        AND cost.city_name = zhuanhua.city
        FULL JOIN
        (SELECT
                report_date,
                media_type,
                customer_intent_city_name as city,
                aid,
                app_id,
                sum(xs_cnt_time) AS xs_cnt_time,
                sum(sh_cnt_time) AS sh_cnt_time,
                sum(dk_cnt_time) AS dk_cnt_time,
                sum(rg_cnt_time) AS rg_cnt_time
        FROM
            tmp_etl.app_zhuanhua_time
        GROUP BY
                report_date,
                media_type,
                aid,
                app_id,
                customer_intent_city_name
        )zhuanhua_time
        --时间维度的转化
        ON cost.report_date = zhuanhua_time.report_date
        AND cost.plan_id = zhuanhua_time.aid
        AND cost.media_type_name = zhuanhua_time.media_type
        AND cost.app_type = zhuanhua_time.app_id
        AND cost.city_name = zhuanhua_time.city
        )final
    GROUP BY
            report_date,
            account_name,
            plan_id,
            media_type_name,
            app_type,
            city
    )ppp
LEFT JOIN plan
ON ppp.plan_id = plan.plan_id

WHERE report_date>='2020-09-01'
;


DROP VIEW IF EXISTS tmp_bi.app_plan_area_zhuanhua;
CREATE VIEW IF NOT EXISTS tmp_bi.app_plan_area_zhuanhua (
report_date                                                    COMMENT '报告日期',   
account_name                                                   COMMENT '账户名称',   
plan_id                                                        COMMENT '计划ID',   
city                                                           COMMENT '投放城市',  
media_type_name                                                COMMENT '媒体类型',   
app_type                                                       COMMENT 'APP类型',   
show_num                                                       COMMENT '展示',   
click_num                                                      COMMENT '点击',   
bill_cost                                                      COMMENT '消耗',   
cost                                                           COMMENT '真实消耗',   
jh_cnt                                                         COMMENT '激活数',   
xs_cnt                                                         COMMENT '线索数',   
sh_cnt                                                         COMMENT '上户数',   
dk_cnt                                                         COMMENT '带看数',   
rg_cnt                                                         COMMENT '认购金额',   
xs_cnt_time                                                    COMMENT '线索数',   
sh_cnt_time                                                    COMMENT '上户数',   
dk_cnt_time                                                    COMMENT '带看数',   
rg_cnt_time                                                    COMMENT '认购金额',   
plan_name                                                      COMMENT '计划名称')
COMMENT '今日头条带城市统计数据'
AS SELECT * FROM julive_app.app_market_plan_area_zhuanhua;

