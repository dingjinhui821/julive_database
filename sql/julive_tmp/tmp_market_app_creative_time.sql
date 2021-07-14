set hive.execution.engine=spark;
set spark.app.name=tmp_market_app_creative_time;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

-- 将原逻辑临时表创意展点消进行抽取
insert OVERWRITE TABLE tmp_etl.creative_report_app
	SELECT
	report_date,
	account_name,
	dsp_account_id ,
	plan_id ,
	plan_name ,
	creative_id ,
	creative_name ,
	title,
	media_type_name,
	app_type,
	sum(show_num) as show_num,
	sum(click_num) as click_num,
	sum(bill_cost) as bill_cost,
	sum(cost) as cost
	FROM
	(
		SELECT
		report_date,
		account_name,
		creative_app.dsp_account_id,
		if(media_type_name = '今日头条' or media_type_name = '快手' or media_type_name = '腾讯智汇推',unit_id,
			if(media_type_name = '百度信息流' ,plan_id,null)) as plan_id,
		if(media_type_name = '今日头条' or media_type_name = '快手' or media_type_name = '腾讯智汇推',unit_name,
			if(media_type_name = '百度信息流' ,plan_name,null)) as plan_name,
		if(media_type_name = '今日头条' or media_type_name = '快手',creative_id,
			if(media_type_name = '百度信息流',unit_id,null)) as creative_id,
		if(media_type_name = '今日头条' or media_type_name = '快手',creative_name,
			if(media_type_name = '百度信息流',unit_name,null)) as creative_name,
		if(media_type_name = '今日头条' or media_type_name = '快手',title,
			if(media_type_name = '百度信息流',unit_name,null)) as title,
		media_type_name,
		app_type,
		show_num,
		click_num,
		bill_cost,
		(bill_cost/(1+dsp_account_rebate.rebate)) as cost
		FROM
		(
			SELECT
			from_unixtime( report_date,'yyyy-MM-dd' ) as report_date,
			account_name,
			dsp_account_id,
			cast(plan_id as String) as plan_id,
			plan_name,
			creative_id,
			creative_name,
			unit_id,
			unit_name,
			title,
			media_type_name,
			app_type,
			sum(show_num) as show_num,
			sum(click_num) as click_num,
			sum(bill_cost) as bill_cost
			from ods.dsp_creative_report
			left join julive_dim.dim_dsp_account on dsp_creative_report.dsp_account_id = dim_dsp_account.id
			where from_unixtime( report_date ,'yyyy') >= '2019'
			AND (media_type_name = '今日头条'
				OR media_type_name = '百度信息流'
				OR media_type_name = '快手'
				OR media_type_name = '应用市场（安卓）'
				OR media_type_name = '腾讯智汇推')
			AND product_type_name = 'APP渠道'
			GROUP BY
			from_unixtime( report_date,'yyyy-MM-dd' ),
			account_name,
			dsp_account_id,
			cast(plan_id as String),
			plan_name,
			creative_id,
			creative_name,
			unit_id,
			unit_name,
			title,
			media_type_name,
			app_type
		)creative_app
		left join
		(select
			dsp_account_id,
			from_unixtime(start_date,'yyyy-MM-dd') as start_date,
			from_unixtime(end_date,'yyyy-MM-dd') as end_date,
			rebate
			from ods.dsp_account_rebate
			where status = 1 
		)dsp_account_rebate
		on creative_app.dsp_account_id=dsp_account_rebate.dsp_account_id
		where creative_app.report_date BETWEEN dsp_account_rebate.start_date and dsp_account_rebate.end_date
		AND creative_app.report_date >= '2020-07-09'
	)creative_app_rebate
	GROUP by 
	report_date,
	account_name,
	dsp_account_id ,
	plan_id ,
	plan_name ,
	creative_id ,
	creative_name ,
	title,
	media_type_name,
	app_type
	;

-- 计划明细
insert overwrite table tmp_etl.plan
	select
	plan_id,
	plan_name
	from
	(
		SELECT 
		plan_id,
		plan_name,
		row_number() over(PARTITION BY plan_id ORDER BY report_date desc) as rank
		from tmp_etl.creative_report_app
		where plan_id is not null and plan_id != '0'
		union all 
		SELECT
		plan_id,
		plan_name,
		row_number() over(PARTITION BY plan_id ORDER BY report_date desc) as rank
		FROM
		julive_fact.fact_android_app_store_cost_indi
		where account_id = '747' or account_id = '748'
	)app_huawei
	where plan_id is not null and plan_id != '0' and rank = 1;

--创意明细
insert overwrite table tmp_etl.creative
	SELECT
	creative_id,
	title
	FROM
	(
		SELECT
		creative_id,
		title,
		row_number() over(PARTITION by creative_id order by report_date desc) as rank
		from tmp_etl.creative_report_app
		where creative_id !=0 and creative_id is not null and report_date>='2020-07-09'
	)app_creative
	where rank = 1;

-- 将原逻辑临时表zhuanhua进行抽取
insert overwrite table tmp_etl.creative_app_jihuo
	SELECT
	DISTINCT
	p1.global_id,
	p1.city,
	p1.utm_source,
	p1.install_date_time,
	yw_order.create_date,
	yw_order.distribute_date,
	yw_order.first_see_date,
	yw_order.first_subscribe_date,
	cj_agency.channel_type_name,
	cj_agency.app_id,
	p1.aid,
	p1.cid,
	global_id_exploded_hbase_2_hive.id as julive_id,
	yw_order.clue_id as order_id,
	if(yw_order.is_distribute = 1,yw_order.clue_id,NULL) as sh_order_id,
	if(yw_order.see_num != 0,yw_order.clue_id,NULL) as dk_order_id,
	if(yw_order.subscribe_num != 0,yw_order.clue_id,NULL) as rg_order_id,
	yw_order.subscribe_contains_cancel_ext_income
	FROM
	(SELECT
		install_date_time ,
		`$city` as city,
		if(if(`$utm_source` is not null and `$utm_source`!='',`$utm_source`,channel) is  null or if(`$utm_source` is not null and `$utm_source`!='',`$utm_source`,channel)='',"IOS",if(`$utm_source` is not null and `$utm_source`!='',`$utm_source`,channel)) as utm_source,
		global_id ,
		coalesce(aid,`$utm_campaign`,plan_id,adgroup_id) as aid,
		coalesce(cid,`$utm_content`) as cid
		from
		(select
			dwd_appinstall_channel_match_by_global.*,
			plan.plan_id as adgroup_id
			from dwd.dwd_appinstall_channel_match_by_global
			left join tmp_etl.plan
			on dwd_appinstall_channel_match_by_global.adgroup_name = plan.plan_name
			)dwd_appinstall_channel_match_by_global

		WHERE to_date(install_date_time) >= '2020-07-09'
	)p1
	LEFT JOIN ods.cj_agency
	ON p1.utm_source = cj_agency.utm_source
	LEFT JOIN julive_fact.global_id_exploded_hbase_2_hive
	ON p1.global_id = global_id_exploded_hbase_2_hive.global_id
	LEFT JOIN julive_fact.fact_clue_full_line_indi yw_order
	ON cast(global_id_exploded_hbase_2_hive.id as BIGINT)= yw_order.user_id;

-- 将原逻辑临时表zhuanhua_time进行抽取
INSERT OVERWRITE TABLE tmp_etl.app_zhuanhua_time
select 
create_datetime,
media_type,
app_id,
cid ,
aid ,
utm_source,
customer_intent_city_name,
count( order_id ) as xs_cnt_time,
SUM( is_distribute ) as sh_cnt_time,
sum( see_num ) as dk_cnt_time,
sum(subscribe_contains_cancel_ext_num) as rg_cnt_time,
sum(subscribe_contains_cancel_ext_income) as rengou_yingshou
from 
(
	select
	order_id ,
	create_datetime,
	media_type,
	if(product_id = '101','安卓',if(product_id = '201','苹果',NULL)) as app_id,
	cid ,
	aid ,
	utm_source,
	creative_order.customer_intent_city_name,
	if(creative_order.distribute_datetime is null,0,creative_order.is_distribute) as is_distribute,
	see_num,
	subscribe_contains_cancel_ext_num,
	subscribe_contains_cancel_ext_income
	FROM 
	(
		SELECT
		id as order_id,
		if(is_distribute = 1,1,0) as is_distribute,
		from_unixtime(create_datetime,'yyyy-MM-dd') as create_datetime,
        from_unixtime(distribute_datetime,'yyyy-MM-dd') as distribute_datetime,
		app_source as utm_source,
		cid,
		aid,
		if(app_type = '安卓','101',if(app_type = '苹果','201',NULL)) as product_id,
		media_type,
		customer_intent_city_name
		FROM tmp_etl.tmp_market_order_info
		WHERE (media_type = '今日头条'
			OR media_type = '百度信息流'
			OR media_type = '快手'
			OR media_type = '应用市场（安卓）'
			OR media_type = '广点通'
		)
		AND from_unixtime(create_datetime,'yyyy-MM-dd')>='2020-07-09'
		AND is_distribute != 16
		AND device_type = 'APP'
	)creative_order
	left join julive_fact.fact_clue_full_line_indi
	on creative_order.order_id = fact_clue_full_line_indi.clue_id
)action
group by
create_datetime,
media_type,
app_id,
cid ,
aid ,
utm_source,
customer_intent_city_name
;
