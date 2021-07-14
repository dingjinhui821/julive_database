set hive.execution.engine=spark;
set spark.app.name=app_market_full_process_quick;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;


--   功能描述：市场各环节数据
--   输 入 表：
-- julive_fact.fact_dsp_sem_keyword_report_day_dtl         --sem展点消    
-- julive_dim.dim_dsp_account_history                      --账户全量快照
-- julive_fact.fact_dsp_creative_report_dtl                --dsp创意报告
-- julive_dim.dim_dsp_account_rebate                       --投放返点
-- julive_fact.fact_android_app_store_cost_indi            --安卓应用商店
-- julive_app.app_market_offline_conversion_market         --线下指标统计
-- julive_dim.dim_channel_info                             --渠道维表


--   输 出 表：julive_app.app_market_full_process_quick
-- 
--   创 建 者： 杨立斌  18612857267
--   创建日期： 2021/06/30 15:41
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容




WITH cost AS
(SELECT
	report_date,
	if(media_type_name = '微信公众号','微信',media_type_name) as media_type_name,
	product_type_name,
	device_name,
	dsp_account_id,
	account,
	show_num,
	click_num,
	bill_cost,
	cost  
	FROM
	(SELECT
		report_date,
		media_type_name,
		product_type_name,
		device_name,
		dsp_account_id,
		account,
		sum(show_num) as show_num,
		sum(click_num) as click_num,
		sum(bill_cost) as bill_cost,
		sum(cost) as cost
		FROM
		(SELECT
			market_sem_tmp.report_date,
			dsp_account.media_type_name,
			dsp_account.product_type_name,
			market_sem_tmp.device_name,
			market_sem_tmp.dsp_account_id,
			dsp_account.account,
			show_num,
			click_num,
			bill_cost,
			bill_cost / (1 + coalesce(rebate.rebate,0)) as cost
			FROM 
			(select
				account_id as dsp_account_id,
				report_date,
				IF(device_id = 1,'PC',IF(device_id = 2,'M',NULL)) AS device_name,
				show_num,
				click_num,
				bill_cost
				from julive_fact.fact_dsp_sem_keyword_report_day_dtl
			)market_sem_tmp
			left join
			(select
				media_type_name,
				product_type_name,
				id,
				account,
				p_date
				from julive_dim.dim_dsp_account_history
			)dsp_account
			on market_sem_tmp.dsp_account_id=dsp_account.id
			and to_date(market_sem_tmp.report_date)=to_date(dsp_account.p_date)
			left join
			(SELECT
				*
				FROM julive_dim.dim_dsp_account_rebate
			)rebate
			ON market_sem_tmp.dsp_account_id = rebate.dsp_account_id and market_sem_tmp.report_date = rebate.rebate_date
		)a
		GROUP BY
		report_date,
		media_type_name,
		product_type_name,
		device_name,
		dsp_account_id,
		account

		UNION ALL

		SELECT
		report_date,
		media_type_name,
		product_type_name,
		device_name,
		dsp_account_id,
		account,
		sum(show_num) as show_num,
		sum(click_num) as click_num,
		sum(bill_cost) as bill_cost,
		sum(cost) as cost
		FROM
		(SELECT
			fact_dsp_creative_report_dtl.report_date ,
			dsp_account.media_type_name,
			dsp_account.product_type_name,
			if(dsp_account.app_type is null,'M',dsp_account.app_type) as device_name,
			fact_dsp_creative_report_dtl.dsp_account_id,
			dsp_account.account,
			show_num,
			click_num,
			bill_cost,
			(bill_cost/(1+coalesce(dsp_account_rebate.rebate,0))) as cost
			FROM julive_fact.fact_dsp_creative_report_dtl
			left join
			(select
				media_type_name,
				product_type_name,
				id,
				account,
				p_date,
				app_type 
				from julive_dim.dim_dsp_account_history
			)dsp_account
			on fact_dsp_creative_report_dtl.dsp_account_id=dsp_account.id and fact_dsp_creative_report_dtl.report_date=dsp_account.p_date
			LEFT JOIN
			(SELECT
				dsp_account_id,
				rebate_date,
				start_date,
				end_date,
				rebate
				FROM julive_dim.dim_dsp_account_rebate
			)dsp_account_rebate
			ON fact_dsp_creative_report_dtl.dsp_account_id=dsp_account_rebate.dsp_account_id and fact_dsp_creative_report_dtl.report_date = dsp_account_rebate.rebate_date
			WHERE (dsp_account.product_type_name = 'APP渠道'
				OR dsp_account.product_type_name = 'feed'
				OR dsp_account.product_type_name = '小程序')
		)a
		GROUP BY
		report_date,
		media_type_name,
		product_type_name,
		device_name,
		dsp_account_id,
		account

		UNION ALL
		SELECT
		report_date,
		media_type_name,
		'APP渠道' AS product_type_name,
		'安卓' AS device_name,
		account_id as dsp_account_id,
		account,
		sum(show_num) as show_num,
		sum(click_num) as click_num,
		sum(bill_cost) as bill_cost,
		sum(cost) as cost
		FROM julive_fact.fact_android_app_store_cost_indi
		GROUP BY
		report_date,
		media_type_name,
		account_id,
		account
	)cost
	WHERE report_date>='2019-01-01')


insert overwrite table julive_app.app_market_full_process_quick
	SELECT
	coalesce(cost.report_date,process.report_date) AS report_date,
	coalesce(cost.media_type_name,process.media_type) as media_type,
	coalesce(cost.product_type_name,process.product_type) as product_type,
	coalesce(cost.device_name,process.device_type) as device_type,
	sum(show_num) as show_num,
	sum(click_num) as click_num,
	sum(bill_cost) as bill_cost,
	sum(cost) as cost,
	sum(xs_cnt) as xs_cnt,
	sum(sh_cnt) as sh_cnt,
	sum(dk_cnt) as dk_cnt,
	sum(rg_cnt) as rg_cnt,
	sum(qy_cnt) as qy_cnt,
	sum(rengou_yingshou) as rengou_yingshou,
	sum(rengou_yingshou_net) as rengou_yingshou_net,
	sum(qianyue_yingshou) as qianyue_yingshou,
	sum(400_xs_cnt) as 400_xs_cnt,
	sum(online_dk_cnt) as online_dk_cnt
	FROM
	(SELECT
		report_date,
		media_type_name,
		product_type_name,
		device_name,
		sum(show_num) as show_num,
		sum(click_num) as click_num,
		sum(bill_cost) as bill_cost,
		sum(cost) as cost
		FROM cost
		GROUP BY
		report_date,
		media_type_name,
		product_type_name,
		device_name)cost
	FULL JOIN
	(SELECT
		report_date,
		product_type,
		media_type,
		if (device_type = 'APP',app_type,device_type) as device_type,
		sum(xs_cnt) as xs_cnt,
		sum(sh_cnt) as sh_cnt,
		sum(dk_cnt) as dk_cnt,
		sum(rg_cnt) as rg_cnt,
		sum(qy_cnt) as qy_cnt,
		sum(rengou_yingshou) as rengou_yingshou,
		sum(rengou_yingshou_net) as rengou_yingshou_net,
		sum(qianyue_yingshou) as qianyue_yingshou,
		sum(400_xs_cnt) as 400_xs_cnt,
		sum(online_dk_cnt) as online_dk_cnt
		FROM
		(
			select 
			app_market_offline_conversion_market.*,
			dim_channel_info.module_name as product_type,
			dim_channel_info.device_name as device_type,
			dim_channel_info.app_type_name as app_type
			from julive_app.app_market_offline_conversion_market 
			join julive_dim.dim_channel_info 
			on app_market_offline_conversion_market.channel_id=dim_channel_info.channel_id
		) app_market_offline_conversion_market 
		WHERE report_date >= '2019-01-01'
		GROUP BY
		report_date,
		product_type,
		media_type,
		if (device_type = 'APP',app_type,device_type)
	)process
	ON cost.report_date = process.report_date
	AND cost.media_type_name = process.media_type
	AND cost.product_type_name = process.product_type
	AND cost.device_name = process.device_type
	GROUP BY
	coalesce(cost.report_date,process.report_date),
	coalesce(cost.media_type_name,process.media_type),
	coalesce(cost.product_type_name,process.product_type),
	coalesce(cost.device_name,process.device_type)
;


DROP VIEW IF EXISTS tmp_bi.market_full_process_quick;
CREATE VIEW IF NOT EXISTS tmp_bi.market_full_process_quick (
report_date                                                    COMMENT '报告日期',   
media_type                                                     COMMENT '账户名称',   
product_type                                                   COMMENT '计划ID',   
device_type                                                    COMMENT '创意ID', 
show_num                                                       COMMENT '展示',   
click_num                                                      COMMENT '点击',   
bill_cost                                                      COMMENT '消耗',   
cost                                                           COMMENT '真实消耗',
xs_cnt                                                         COMMENT '线索数',   
sh_cnt                                                         COMMENT '上户数',   
dk_cnt                                                         COMMENT '带看数',   
rg_cnt                                                         COMMENT '认购金额',   
qy_cnt                                                         COMMENT '签约数量',  
rengou_yingshou                                                COMMENT '认购金额',
rengou_yingshou_net                                            COMMENT '认购(不含退)-含外联佣金',
qianyue_yingshou                                               COMMENT '原合同预测总收入',
400_xs_cnt                                                     COMMENT '400线索量',
online_dk_cnt                                                  COMMENT '线上带看量')
COMMENT '市场全流程（渠道城市）'
AS SELECT * FROM julive_app.app_market_full_process_quick;


