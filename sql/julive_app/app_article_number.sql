set hive.exectuion.engine=spark;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.yarn.queue=etl;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

drop table if exists tmp_dev_1.tmp_article_number;
 create table tmp_dev_1.tmp_article_number as

 select

 article_id,
 click_num

 from julive_app.app_article_number
 where pdate = where pdate = regexp_replace(date_add(current_date(),-2),'-','');

insert overwrite table julive_app.app_article_number partition(pdate)
select 
   t.article_id as article_id,
   sum(t.click_num) as click_num,
   regexp_replace(date_add(current_date(),-1),'-','') as pdate
   from 
   (SELECT 
      get_json_object(t1.properties,'$.article_id')  as article_id,
      count(1) as click_num 
      from julive_fact.fact_event_dtl t1  
      where 
        event = 'e_page_view' and 
        topage = 'p_article_details' and
        product_id =2 and
        pdate = regexp_replace(date_add(current_date(),-1),'-','') and 
        get_json_object(t1.properties,'$.comjia_platform_id') in (201,101)
        group by get_json_object(t1.properties,'$.article_id')
    union all
    select
      article_id,
      click_num
      from tmp_dev_1.tmp_article_number
    )t
   where t.article_id is not null
   group by t.article_id;


