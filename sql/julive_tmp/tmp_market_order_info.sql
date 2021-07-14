set hive.execution.engine=spark;
set spark.app.name=tmp_market_order_info;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;

--   功能描述：APP创意层级展点消以及业务行为明细
--   输 入 表：
--         julive_dim.dim_clue_base_info                                 -- 经过清洗的线索表
--         julive_dim.dim_employee_info                                  -- 员工维度表
--         julive_fact.fact_market_order_rel_appinstall                  -- 激活事实表
--         ods.yw_order_tags                                             -- 订单标签
--         julive_dim.dim_channel_info                                   -- 渠道维度表
--         dm.sh_quality_order_score                                     -- 上户质量分数表
--         ods.yw_sys_number_talking                                     -- 通话明细表
--         dwd.consultant_called_log_clue_report                         -- 咨询师通话记录明细
--         ods.yw_subscribe                                              -- 认购表


--   输 出 表：tmp_etl.tmp_market_order_info
--               
-- 
--   创 建 者：  杨立斌  18612857267
--   创建日期： 2021/06/02 14:16
--  ---------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE tmp_etl.tmp_market_order_info
SELECT
yw_order.id as id,
unix_timestamp(yw_order.create_date,'yyyy-MM-dd') as create_datetime,
yw_order.channel_id as channel_id,
yw_order.source as source,
yw_order.is_distribute,
yw_order.city_id,
dim_channel_info.device_name as device_type,  
coalesce(p4.media_name,dim_channel_info.media_name) as media_type,
unix_timestamp(yw_order.distribute_date,'yyyy-MM-dd'),
sh_quality_order_score.probs,
yw_order.customer_intent_city_id,
default.decode_item(yw_order.user_mobile) as user_mobile,
yw_order.clue_score,
yw_order.intent_tc AS intent,
yw_order.intent_low_time AS intent_low_datetime,
if(yw_sys_number_talking.call_duration > 0,1,0) AS jietong_sh_day,
first_call_duration.first_call_duration,
if(is_short_alloc=1 and short_alloc_type=1,'路径缩短',
	if(is_short_alloc=1 and short_alloc_type=1,'路径缩短',
		if(yw_order.op_type=900405 or yw_order.op_type=900406,'路径缩短',
			if(yw_order.is_distribute=1,'非路径缩短',null)))) AS is_short,
dim_channel_info.module_name as product_type_name,
dim_channel_info.app_type_name as app_type,
if(yw_order.is_distribute != 16 AND (yw_order.source = 9 or yw_order.source = 10),1,0) as is_400,
if(yw_order.is_distribute != 16 AND yw_order.source != 9 AND yw_order.source != 10,1,0) as is_leavephone,
yw_order.customer_intent_city_name as customer_intent_city_name,
dim_channel_info.city_name as channel_city_name,
dim_channel_info.channel_name,
IF(yw_order.is_distribute=1,"已分配",
	IF(yw_order.is_distribute=99,"其它",
		IF(yw_order.is_distribute=2,"谈合作",
			IF(yw_order.is_distribute=3,"超区域",
				IF(yw_order.is_distribute=4,"不关注楼盘",
					IF(yw_order.is_distribute=5,"必须找售楼处",
						IF(is_distribute=6,"关注二手房",
							IF(is_distribute=7,"之前已上户",
								IF(is_distribute=8,"不愿留电话",
									IF(is_distribute=9,"电话无法打通",
										IF(is_distribute=10,"空号错号",
											IF(is_distribute=11,"非业务来电",
												IF(is_distribute=12,"骚扰",
													IF(is_distribute=13,"网站新上户",
														IF(is_distribute=14,"拒绝居理新房服务",
															IF(is_distribute=15,"已通过其他渠道购买房屋",
																IF(is_distribute=16,"系统测试",
																	IF(is_distribute=17,"房产中介",
																		IF(is_distribute=18,"预算不足",
																			IF(is_distribute=19,"只关注售罄",
																				IF(is_distribute=20,"待上户",
																					IF(is_distribute=21,"资质不足",
																						IF(is_distribute=22,"投诉",
																							IF(is_distribute=23,"非合作",
																								IF(is_distribute=24,"大北京测试",
																									IF(is_distribute=25,"当地方言",
																										IF(is_distribute=26,"城市分配错误",
																											IF(is_distribute=27,"路径缩短计划",
																												IF(is_distribute=28,"首次联系不成功",
																													IF(is_distribute=29,"二次联系不成功",
																														IF(is_distribute=30,"三次联系不成功",
																															IF(is_distribute=31,"四次联系不成功",
																																IF(is_distribute=32,"最终拒接",
																																	IF(is_distribute=33,"最终占线/无人接听",
																																		IF(is_distribute=34,"最终关机",
																																			IF(is_distribute=35,"最终空号",
																																				IF(is_distribute=36,"非本人注册",
																																					IF(is_distribute=37,"无意点错",
																																						IF(is_distribute=38,"关注租房",
																																							IF(is_distribute=39,"重复无效线索",
																																								IF(is_distribute=40,"神秘客户",
																																									IF(is_distribute=44,"推销",
																																										IF(is_distribute=43,"接通后秒挂",
																																											IF(is_distribute=49,"联系不成功-接通后无声",
																																												IF(is_distribute=50,"联系不成功-通话中客户挂断",
																																													IF(is_distribute=51,"其他原因-满意度评价追回",
																																														IF(is_distribute=42,"振铃被挂断",
																																															IF(is_distribute=53,"恶意刷量",
																																																IF(is_distribute=41,"振铃超时",
																																																	IF(is_distribute=45,"用户已设置免打扰",
																																																		IF(is_distribute=46,"客户打错电话",
																																																			IF(is_distribute=47,"投诉","其它")))))))))))))))))))))))))))))))))))))))))))))))))))) secend_reason,
yw_order.distribute_category_desc as first_reason,
rengou_date,
yw_order.emp_id,
yw_order.user_id,
coalesce(fact_market_order_rel_appinstall.utm_source,fact_market_order_rel_appinstall.channel) as app_source,
aid,
cid,
fact_market_order_rel_appinstall.install_date_time,
yw_order.org_id,
yw_order.org_name,
dim_employee_info.full_type_tc as full_type,
subscribe_name,
subscribe_age,
subscribe_sex,
yw_order.total_price_max,   
case when yw_order.from_source = 1 and yw_order.org_id=48  then '自营业务'
when yw_order.from_source=1 and yw_order.org_id!=48  then '内部加盟业务'
when yw_order.from_source=2 then '乌鲁木齐项目'
when yw_order.from_source=3 then '二手房中介'
when yw_order.from_source=4 then '外部加盟商'
else '其他'
end as yw_line,
order_tag.order_tag 
FROM
(
	SELECT
	clue_id as id,
	emp_id,
	user_id,
	create_date,
	channel_id,
	source,
	distribute_date,
	dim_clue_base_info.city_id,
	org_id,
	org_name,
	from_source,
	customer_intent_city_id,
	customer_intent_city_name,
	user_mobile,
	op_type,
	intent_tc,
	intent_low_time,
	is_short_alloc,
	short_alloc_type,
	is_distribute,
	total_price_max,
	clue_score,
	distribute_category_desc
	FROM
	julive_dim.dim_clue_base_info 
	where dim_clue_base_info.city_id not in(select city_id from julive_dim.dim_wlmq_city table2)
)yw_order 
left join julive_dim.dim_employee_info
on yw_order.emp_id = dim_employee_info.emp_id
and yw_order.distribute_date = CONCAT(SUBSTR(dim_employee_info.pdate,1,4),'-',SUBSTR(dim_employee_info.pdate,5,2),'-',SUBSTR(dim_employee_info.pdate,7,2))

left join julive_fact.fact_market_order_rel_appinstall
on yw_order.id = fact_market_order_rel_appinstall.order_id

LEFT JOIN 
(SELECT
	order_id,
	title AS order_tag
	FROM
	(select y1.order_id,
		y1.tag_id,
		y2.title,
		row_number() over(partition by y1.order_id order by y1.create_datetime desc) as rank
		from ods.yw_order_tags y1 left
		join ods.yw_tags y2 
		on y1.tag_id=y2.id 
		where y2.title = 'A'
		OR y2.title = 'B'
		OR y2.title = 'C'
		OR y2.title = 'S'
	)a
	WHERE rank = 1
)order_tag
ON order_tag.order_id = yw_order.id

LEFT JOIN
(SELECT
	DISTINCT
	utm_source,
	media_name 
	FROM julive_dim.dim_channel_info
	WHERE utm_source is not null
)p4
ON coalesce(fact_market_order_rel_appinstall.utm_source,fact_market_order_rel_appinstall.channel)=p4.utm_source


LEFT JOIN
(SELECT
	order_id,
	sum(probs)/count(order_id) as probs
	FROM dm.sh_quality_order_score
	GROUP BY
	order_id
)sh_quality_order_score
ON yw_order.id=sh_quality_order_score.order_id

LEFT JOIN
(SELECT
	to_date(release_time) as release_time,
	order_id,
	count(id) as tonghua_num,
	sum(call_duration) as call_duration
	FROM ods.yw_sys_number_talking
	WHERE call_result = 'ANSWERED'
	GROUP BY
	to_date(release_time),
	order_id
	HAVING release_time IS NOT NULL
	AND order_id != 0
)yw_sys_number_talking
ON yw_order.id = yw_sys_number_talking.order_id
AND yw_order.distribute_date = yw_sys_number_talking.release_time

LEFT JOIN
(SELECT
	order_id,
	first_call_duration/60 AS first_call_duration
	FROM
	(SELECT
		order_id ,
		first_call_duration ,
		row_number() over(partition by order_id order by first_call_time) AS r
		FROM dwd.consultant_called_log_clue_report
		WHERE first_call_duration != 0
	)a
	WHERE r = 1
)first_call_duration
ON yw_order.id = first_call_duration.order_id

LEFT JOIN julive_dim.dim_channel_info
ON yw_order.channel_id = dim_channel_info.channel_id

LEFT JOIN
(SELECT
	order_id as order_id,
	rengou_date,
	subscribe_name,
	subscribe_age,
	subscribe_sex
	FROM
	(SELECT
		id as rengou_id,
		order_id,
		from_unixtime(subscribe_datetime,'yyyy-MM-dd') as rengou_date,
		subscribe_name ,
		(2020-cast(substr(default.decode_item(subscribe_identity_number),7,4) as int)+1) as subscribe_age,
		if(pmod(cast(substr(default.decode_item(subscribe_identity_number),17,1) as int),2) = 1,'男',
			if(pmod(cast(substr(default.decode_item(subscribe_identity_number),17,1) as int),2) = 0,'女',null)) as subscribe_sex,
		row_number() over(PARTITION BY order_id ORDER BY subscribe_datetime) as r
		FROM ods.yw_subscribe
		LEFT JOIN ods.cj_project
		ON yw_subscribe.project_id = cj_project.project_id
		WHERE subscribe_type in (1,4)
		AND yw_subscribe.status in (1,2)
	)yw_subscribe
	WHERE r = 1
)rg
ON yw_order.id = rg.order_id
;