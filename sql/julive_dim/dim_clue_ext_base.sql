
set hive.execution.engine=spark;
set spark.app.name=topic_dws_order_day;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=12;

set etl_date = '${hiveconf:etlDate}';


-- set etl_yestoday = '${hiveconf:etlYestoday}'; 

-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：DIM-扩展线索维度表

--   输 入 表 ：
--         julive_dim.dim_clue_base_info                    -- 线索维度基表-包含多条业务线数据 from_source区分
--         julive_fact.fact_market_order_rel_appinstall     -- 订单关联AppInstall表
--         julive_dim.dim_employee_info                     -- 员工信息维度表
--         julive_dim.dim_channel_info                      -- 渠道维度表
--         ods.yw_order_tags                                -- 
--         ods.yw_tags
--         

--   输 出 表：julive_dim.dim_clue_ext_base
-- 
--   创 建 者：  薛理  15996981324
--   创建日期： 2021/06/23 14:16
--  ---------------------------------------------------------------------------------------
--   修改日志：
--   修改日期： 修改人   修改内容

insert overwrite TABLE julive_dim.dim_clue_ext_base
select 
    t1.clue_id  ,             
    t1.create_date,          
    t1.create_time,        
    t1.channel_id,      
    t1.org_id        ,        
    t1.org_type       ,       
    t1.org_name       ,
    t1.from_source,
    t1.from_source_detail,
    t1.city_id      ,         
    t1.city_name       ,      
    t1.source              ,  
    t1.user_id          ,
    t1.intent,
    t1.intent_tc,
    if(is_short_alloc=1 and short_alloc_type=1,'路径缩短',
        if(is_short_alloc=1 and short_alloc_type=1,'路径缩短',
        if(t1.op_type=900405 or t1.op_type=900406,'路径缩短',
        if(t1.is_distribute=1,'非路径缩短',null)))) AS is_short,
    t1.customer_intent_city_id,
    t1.customer_intent_city_name,
    t1.user_name             ,
    t1.user_mobile           ,
    t1.sex                   ,
    t1.creator               ,
    t1.emp_id                ,
    t1.emp_name              ,
    t1.is_400_called         ,
    t1.is_distribute         ,
    coalesce(t8.media_name,t2.media_name)  as media_name,
    t2.module_name           ,
    t2.device_name           ,
    t2.app_type_name         ,
    t2.city_name as channel_city_name     ,
    t2.channel_name          ,
    translate(t1.distribute_tc,'不分配原因：','') as  secend_reason,
	  t1.distribute_category_desc as first_reason,
    coalesce(t5.utm_source,t5.channel,t5.channel_name) as app_source            ,
    to_date(t5.install_date_time) as install_date          ,
    t5.install_date_time as install_time          ,
    if(t5.`$city` != '',t5.`$city`,dim_city.city_name) as install_city_name,
    if(substr(t5.product_id,1,3) = 101,'安卓',if(substr(t5.product_id,1,3) = 201,'苹果',NULL)) as install_app_type_name,
    t5.aid                   ,
    t5.cid                   ,
    t6.full_type             ,
    case 
        when t1.from_source = 1 and t1.org_id=48  then '自营业务'
        when t1.from_source=1 and t1.org_id!=48  then '内部加盟业务'
        when t1.from_source=2 then '乌鲁木齐项目'
        when t1.from_source=3 then '二手房中介'
        when t1.from_source=4 then '外部加盟商'
        else '其他'
        end as yw_line ,
    t7.order_tag ,
    current_timestamp() as etl_time              
from julive_dim.dim_clue_base_info t1
left join julive_dim.dim_channel_info t2 on t1.channel_id = t2.channel_id
left join julive_fact.fact_market_order_rel_appinstall t5 on t1.clue_id = t5.order_id
LEFT JOIN julive_dim.dim_city ON t5.select_city = cast(dim_city.city_id as string)
left join julive_dim.dim_employee_info t6 on t1.emp_id = t6.emp_id
    and translate(t1.distribute_date,'-','') =  t6.pdate
left join (
    SELECT
       order_id,
       title AS order_tag
    FROM(
        select y1.order_id,
            y1.tag_id,
            y2.title,
            row_number() over(partition by y1.order_id order by y1.create_datetime desc) as rank
        from ods.yw_order_tags y1 
        left join ods.yw_tags y2  on y1.tag_id=y2.id 
        where y2.title in ( 'A', 'B', 'C', 'S')
    )a WHERE rank = 1
)t7
ON t1.clue_id = t7.order_id
left join (
    SELECT
        utm_source,
        max(media_name) as media_name
    FROM julive_dim.dim_channel_info
    WHERE utm_source is not null
    group by utm_source
)t8 ON coalesce(t5.utm_source,t5.channel,t5.channel_name)=t8.utm_source
;