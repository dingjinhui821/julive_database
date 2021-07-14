set hive.execution.engine=spark;
set spark.app.name=app_market_creative_zhuanhua;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：APP创意层级展点消以及业务行为明细
--   输 入 表：
--         tmp_etl.creative_report_app                        -- app创意展点消
--         julive_fact.fact_android_app_store_cost_indi       -- 安卓应用市场展点消
--         tmp_etl.creative_app_jihuo                         -- app激活以及业务行为
--         tmp_etl.app_zhuanhua_time                          -- 业务行为和认购金额
--         tmp_etl.plan                        			      -- 计划维度
--         tmp_etl.creative                                   -- 创意维度


--   输 出 表：julive_app.app_market_creative_zhuanhua
--            julive_app.app_market_creative_zhuanhua_plan_not_sh	  
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/05/22 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：20210603   将腾讯智汇推去掉


INSERT OVERWRITE TABLE julive_app.app_market_creative_zhuanhua
SELECT
distinct
ppp.report_date,
ppp.account_name,
ppp.plan_id,
ppp.creative_id,
ppp.media_type_name,
ppp.app_type,
ppp.show_num,                                                          
ppp.click_num,
ppp.bill_cost,
ppp.cost,
ppp.jh_cnt,
ppp.xs_cnt,
ppp.sh_cnt,
ppp.dk_cnt,
ppp.rg_cnt,
ppp.xs_cnt_time,
ppp.sh_cnt_time,
ppp.dk_cnt_time,
ppp.rg_cnt_time,
plan.plan_name,
creative.title,
ppp.rengou_yingshou_time,
ppp.rengou_yingshou_uv,
ppp.download_num
FROM
(SELECT
report_date,
account_name,
plan_id,
creative_id,
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
sum(rg_cnt_time) as rg_cnt_time,
sum(rengou_yingshou_time) as rengou_yingshou_time,
sum(rengou_yingshou_uv) as rengou_yingshou_uv,
sum(download_num) as download_num
FROM
(SELECT
coalesce(cost.report_date,zhuanhua.install_date_time,zhuanhua_time.report_date) as report_date,
cost.account_name,
coalesce(cost.plan_id,zhuanhua.aid,zhuanhua_time.aid) AS plan_id,
coalesce(cost.creative_id,zhuanhua.cid,zhuanhua_time.cid) AS creative_id,
coalesce(cost.media_type_name,zhuanhua.channel_type_name,zhuanhua_time.media_type) AS media_type_name,
coalesce(cost.app_type,zhuanhua.app_id,zhuanhua_time.app_id) AS app_type,
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
zhuanhua_time.rg_cnt_time,
zhuanhua.rengou_yingshou_uv,
zhuanhua_time.rengou_yingshou_time,
cost.download_num
FROM
(SELECT
report_date,
account_name,
dsp_account_id,
plan_id,
plan_name,
creative_id,
creative_name,
media_type_name,
app_type,
show_num ,
click_num,
bill_cost,
cost,
download_num
FROM
(
SELECT
report_date,
account_name,
dsp_account_id,
plan_id,
plan_name,
creative_id,
creative_name,
media_type_name,
app_type,
show_num ,
click_num,
bill_cost,
cost,
0 as download_num
FROM
tmp_etl.creative_report_app
union all
SELECT
report_date,
account as account_name,
account_id as dsp_account_id,
plan_id,
plan_name,
null as creative_id,
null as creative_name,
media_type_name,
app_type,
show_num,
click_num,
bill_cost,
cost,
download_num
FROM
julive_fact.fact_android_app_store_cost_indi
where (account_id = '747' or account_id = '748') and report_date >= '2020-07-09'
)app_huawei
)cost
FULL JOIN
(SELECT
to_date(install_date_time) AS install_date_time,
channel_type_name,
if(app_id = '101','安卓',if(app_id = '201','苹果',NULL)) as app_id,
aid,
cid,
count(DISTINCT global_id) AS jh_cnt,
count(DISTINCT if(to_date(install_date_time)<=to_date(create_date),order_id,null)) AS xs_cnt,
count(DISTINCT if(to_date(install_date_time)<=to_date(distribute_date),sh_order_id,null)) AS sh_cnt,
count(DISTINCT if(to_date(install_date_time)<=to_date(first_see_date),dk_order_id,null)) AS dk_cnt,
count(DISTINCT if(to_date(install_date_time)<=to_date(first_subscribe_date),rg_order_id,null)) AS rg_cnt,
sum(subscribe_contains_cancel_ext_income) as rengou_yingshou_uv
FROM
tmp_etl.creative_app_jihuo
GROUP BY
to_date(install_date_time),
channel_type_name,
if(app_id = '101','安卓',if(app_id = '201','苹果',NULL)),
aid,
cid
HAVING channel_type_name = '今日头条'
OR channel_type_name = '百度信息流'
OR channel_type_name = '快手'
OR channel_type_name = '应用市场（安卓）'
OR channel_type_name = '广点通'
)zhuanhua
ON cost.report_date = zhuanhua.install_date_time
AND cost.plan_id = zhuanhua.aid
AND cost.creative_id = zhuanhua.cid
AND cost.media_type_name = zhuanhua.channel_type_name
AND cost.app_type = zhuanhua.app_id
FULL JOIN
(SELECT
report_date,
media_type,
aid,
cid,
app_id,
sum(xs_cnt_time) AS xs_cnt_time,
sum(sh_cnt_time) AS sh_cnt_time,
sum(dk_cnt_time) AS dk_cnt_time,
sum(rg_cnt_time) AS rg_cnt_time,
sum(rengou_yingshou) as rengou_yingshou_time
FROM
tmp_etl.app_zhuanhua_time
GROUP BY
report_date,
media_type,
aid,
cid,
app_id
)zhuanhua_time
ON cost.report_date = zhuanhua_time.report_date
AND cost.plan_id = zhuanhua_time.aid
AND cost.creative_id = zhuanhua_time.cid
AND cost.media_type_name = zhuanhua_time.media_type
AND cost.app_type = zhuanhua_time.app_id

)final
GROUP BY
report_date,
account_name,
plan_id,
creative_id,
media_type_name,
app_type
)ppp
LEFT JOIN tmp_etl.plan
ON ppp.plan_id = plan.plan_id

LEFT JOIN tmp_etl.creative
ON ppp.creative_id = creative.creative_id
;



WITH A AS
(SELECT
report_date,
app_type,
plan_name,
media_type_name,
plan_id,
account_name,
sum(cost) AS cost
FROM julive_app.app_market_creative_zhuanhua
WHERE report_date=DATE_ADD(to_date(current_date()),-3)
and cost is not null
and cost != 0
GROUP BY
report_date,
app_type,
plan_name,
media_type_name,
plan_id,
account_name
)

insert overwrite table julive_app.app_market_creative_zhuanhua_plan_not_sh
SELECT
report_date_A,
plan_name,
split(plan_name,'_')[0] as plan_name_1,
media_type_name,
plan_id,
account_name,
AVG(cost) AS cost,
sum(sh_cnt_time) as sh_cnt_time,
sum(xs_cnt_time) as xs_cnt_time,
sum(jh_cnt) as jh_cnt
FROM
(SELECT
A.report_date AS report_date_A,
A.plan_name,
A.media_type_name,
A.plan_id,
A.account_name,
A.cost,
app_market_creative_zhuanhua.report_date,
app_market_creative_zhuanhua.sh_cnt_time,
app_market_creative_zhuanhua.jh_cnt,
app_market_creative_zhuanhua.xs_cnt_time
FROM A
LEFT JOIN julive_app.app_market_creative_zhuanhua
ON A.app_type = app_market_creative_zhuanhua.app_type
AND A.plan_name = app_market_creative_zhuanhua.plan_name
AND A.media_type_name = app_market_creative_zhuanhua.media_type_name
AND A.plan_id = app_market_creative_zhuanhua.plan_id
AND A.account_name = app_market_creative_zhuanhua.account_name
WHERE app_market_creative_zhuanhua.report_date BETWEEN A.report_date
AND date_add(A.report_date,3)
)A
GROUP BY
report_date_A,
plan_name,
split(plan_name,'_')[0],
media_type_name,
plan_id,
account_name
HAVING sh_cnt_time IS NULL;




DROP VIEW IF EXISTS tmp_bi.app_creative_zhuanhua;
CREATE VIEW IF NOT EXISTS tmp_bi.app_creative_zhuanhua (
report_date                                                    COMMENT '报告日期',   
account_name                                                   COMMENT '账户名称',   
plan_id                                                        COMMENT '计划ID',   
creative_id                                                    COMMENT '创意ID',
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
plan_name                                                      COMMENT '计划名称',
title                                                          COMMENT '标题',
rengou_yingshou_time                                           COMMENT '认购金额',
rengou_yingshou_uv                                             COMMENT '认购金额',
download_num                                                   COMMENT '下载量')
COMMENT '创意层级转化时间维度'
AS SELECT * FROM julive_app.app_market_creative_zhuanhua;


DROP VIEW IF EXISTS tmp_bi.plan_not_sh;
CREATE VIEW IF NOT EXISTS tmp_bi.plan_not_sh (
report_date_a                                                  COMMENT '报告日期', 
plan_name                                                      COMMENT '计划名称', 
plan_name_1                                                    COMMENT '计划名称切割',  
media_type_name                                                COMMENT '媒体类型', 
plan_id                                                        COMMENT '计划ID',
account_name                                                   COMMENT '账户名称',   
cost                                                           COMMENT '真实消耗', 
sh_cnt_time                                                    COMMENT '上户数',
xs_cnt_time                                                    COMMENT '线索数',
jh_cnt                                                         COMMENT '激活数')
COMMENT '创意层级计划转化时间维度'
AS SELECT * FROM julive_app.app_market_creative_zhuanhua_plan_not_sh;
