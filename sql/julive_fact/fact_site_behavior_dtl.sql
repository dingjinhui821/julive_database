--  功能描述：网站行为数据事实表
--  输 入 表：
--        ods.yw_order_website_op                                 -- 网站留电表
--        ods.yw_order_appsite_op                                 -- app留电表
--        julive_dim.dim_clue_base_info                           -- 线索维度基表
--
--
--
--  输 出 表：julive_fact.fact_site_behavior_dtl                    -- 网站行为数据事实表
--
--
--  创 建 者：  蒋胜洋  13971279523  jiangshengyang@julive.com
--  创建日期： 2021/06/30 14:07
--
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--

set hive.execution.engine=spark;
set spark.app.name=fact_site_behavior_dtl;
set mapred.job.name=fact_site_behavior_dtl;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=24;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;

drop table if exists tmp_etl.tmp_fact_site_behavior_dtl;

create table tmp_etl.tmp_fact_site_behavior_dtl as
select
    t1.order_id as clue_id,
    t2.create_time as clue_create_time,
    t2.is_distribute,
    t1.channel_id,
    t1.from_project_id as project_id,
    if(t1.channel_put is not null and t1.channel_put != '',t1.channel_put,'') as channel_put,
    t1.referer,     -- 20210624添加，julive_app.app_feed_channelput_conversion有用到
    t1.product_id,
    null as sub_product_id,
    t1.op_type,
    t1.source as device_id,

    null as media_id,
    if(t1.media_source is null or t1.media_source = '' or t1.media_source = -1,-1,t1.media_source) as media_name,
    if(t1.c_creative is null or t1.c_creative = '' or t1.c_creative = -1,-1,t1.c_creative) as creative_id,
    if(t1.c_keywordid is null or t1.c_keywordid = '' or t1.c_keywordid = -1,-1,t1.c_keywordid) as keyword_id,

    1 as from_source,
    row_number()over(partition by t1.order_id order by if(t1.channel_put is not null and t1.channel_put != '',t1.create_datetime,unix_timestamp('9999-12-31 00:00:00')) asc) as is_first_website,
    from_unixtime(t1.create_datetime) as create_time,
    from_unixtime(t1.update_datetime) as update_time
from ods.yw_order_website_op t1
join julive_dim.dim_clue_base_info t2 on t1.order_id = t2.clue_id
union all
select
    t1.order_id as clue_id,
    t2.create_time as clue_create_time,
    t2.is_distribute,
    t1.channel_id,
    t1.from_project_id as project_id,
    if(t1.channel_put is not null and t1.channel_put != '',t1.channel_put,'') as channel_put,
    null as referer,    -- 20210624添加，julive_app.app_feed_channelput_conversion有用到
    if(t1.product_id != 0,t1.product_id,t1.app_id) as product_id,
    t1.sub_product_id as sub_product_id,
    t1.op_type,
    null as device_id,

    -1 as media_id,
    -1 as media_name,
    -1 as creative_id,
    -1 as keyword_id,

    2 as from_source,
    row_number()over(partition by t1.order_id order by if(t1.channel_put is not null and t1.channel_put != '',t1.create_datetime,unix_timestamp('9999-12-31 00:00:00')) asc) as is_first_website,
    from_unixtime(t1.create_datetime) as create_time,
    from_unixtime(t1.update_datetime) as update_time
from ods.yw_order_appsite_op t1
join julive_dim.dim_clue_base_info t2 on t1.order_id = t2.clue_id
;

insert overwrite table julive_fact.fact_site_behavior_dtl
select
    clue_id,
    clue_create_time,
    is_distribute,
    channel_id,
    project_id,
    channel_put,
    referer,
    product_id,
    sub_product_id,
    op_type,
    device_id,
    media_id,
    media_name,
    creative_id,
    keyword_id,
    from_source,
    is_first_website,
    create_time,
    update_time,
    current_timestamp() as etl_time
from tmp_etl.tmp_fact_site_behavior_dtl
;

drop table if exists tmp_etl.tmp_fact_site_behavior_dtl;

