set hive.execution.engine=spark;
set spark.app.name=fact_android_app_store_cost_indi;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：安卓应用市场展点消(可以根据account_id进行区分；747、748-华为，750-oppo，751-vivo，749-xiaomi)
--   输 入 表：
--         ods.dsp_market_account_plan_report_huawei_t1             -- 华为展点消明细
--         ods.dsp_market_account_plan_report_oppo_t1               -- oppo展点消明细
--         ods.dsp_market_account_plan_report_vivo_t1               -- vivo展点消明细
--         ods.dsp_market_account_plan_report_xiaomi_t1             -- xiaomi展点消明细
--         ods.dsp_account_rebate                        			-- 返点数据
--         julive_dim.dim_dsp_account                               -- 账户表


--   输 出 表：julive_fact.fact_android_app_store_cost_indi  
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/05/17 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

insert overwrite table julive_fact.fact_android_app_store_cost_indi
	SELECT
	report_date,
	account_id,
	account,
	agent,
	plan_id,
	plan_name,
	media_type_name,
	app_type,
	sum(show_num) as show_num,
	sum(click_num) as click_num,
	sum(bill_cost) as bill_cost,
	sum(cost) as cost,
	sum(download_num) as download_num,
	current_timestamp() as etl_time 
	FROM
	(
		SELECT
		android_all.report_date,
		android_all.account_id,
		dim_dsp_account.account,
		dim_dsp_account.agent,
		plan_id,
		plan_name,
		dim_dsp_account.media_type_name,
		app_type,
		show_num,
		click_num,
bill_cost, --jbq 总消耗
download_num,
(bill_cost/(1+dsp_account_rebate.rebate)) as cost --jbq 实际消耗
FROM
(
	SELECT
	from_unixtime(report_date,'yyyy-MM-dd') AS report_date,
	account_id,
	account,
	show_num,
	click_num,
	bill_cost,
	download_num,
	channel as plan_id,
	plan_name
	FROM ods.dsp_market_account_plan_report_huawei_t1
	UNION ALL 
	SELECT
	from_unixtime(report_date,'yyyy-MM-dd') AS report_date,
	account_id,
	account,
	show_num,
	click_num,
	bill_cost,
	download_num,
	plan_id,
	plan_name
	FROM ods.dsp_market_account_plan_report_oppo_t1
	UNION ALL 
	SELECT
	from_unixtime(report_date,'yyyy-MM-dd') AS report_date,
	account_id,
	account,
	show_num,
	click_num,
	bill_cost,
	download_num,
	plan_id,
	plan_name
	FROM ods.dsp_market_account_plan_report_vivo_t1
	UNION ALL 
	SELECT
	from_unixtime(report_date,'yyyy-MM-dd') AS report_date,
	account_id,
	account,
	show_num,
	click_num,
	bill_cost,
	download_num,
	plan_id,
	plan_name
	FROM ods.dsp_market_account_plan_report_xiaomi_t1
)android_all
LEFT JOIN julive_dim.dim_dsp_account
ON android_all.account_id = dim_dsp_account.id
left join
(select
	dsp_account_id,
	from_unixtime(start_date,'yyyy-MM-dd') as start_date,
	from_unixtime(end_date,'yyyy-MM-dd') as end_date,
	rebate 
	from ods.dsp_account_rebate
	where status = 1
)dsp_account_rebate
on android_all.account_id=dsp_account_rebate.dsp_account_id
where android_all.report_date BETWEEN dsp_account_rebate.start_date and dsp_account_rebate.end_date
)PP
	GROUP BY
	report_date,
	account_id,
	account,
	agent,
	plan_id,
	plan_name,
	media_type_name,
	app_type
	;


DROP VIEW IF EXISTS tmp_bi.android_app_store_cost;
CREATE VIEW IF NOT EXISTS tmp_bi.android_app_store_cost (
report_date                                                    COMMENT '报告日期',   
account_id                                                     COMMENT '账户ID',   
account                                                        COMMENT '账户名称',   
agent                                                          COMMENT '代理',
app_type                                                       COMMENT 'APP类型',   
plan_name                                                      COMMENT '计划名称',   
media_type_name                                                COMMENT '媒体类型',   
show_num                                                       COMMENT '展示',   
click_num                                                      COMMENT '点击',   
bill_cost                                                      COMMENT '消耗',   
cost                                                           COMMENT '真实消耗',   
download_num                                                   COMMENT '下载')
COMMENT '安卓应用市场消耗'
AS SELECT  
report_date,
account_id,
account,
agent,
app_type,
plan_name,
media_type_name,
show_num,
click_num,
bill_cost,
cost,
download_num
FROM julive_fact.fact_android_app_store_cost_indi;








