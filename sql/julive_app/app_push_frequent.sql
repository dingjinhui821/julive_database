set mapreduce.job.queuename=root.etl;
set hive.execution.engine=spark;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=4;
set spark.app.name=app_push_frequent;
set spark.yarn.queue=etl;

insert overwrite table tmp_etl.tmp_app_push_frequent
select
    t1.user_id,
    t1.select_city,
    t1.project_list
from
(
    select
        regexp_replace(user_id,'comjia_unique_id_','') as user_id,
        select_city,
        project_list
    from dm.project_list_es
)t1
join
(
    select
        user_id,
        select_city
    from
    (
        select
            user_id,
            select_city,
            row_number() over(partition by user_id order by final_visit_daytime desc) r
        from
        (
            select
                comjia_unique_id as user_id,
                select_city,
                latest_select_city,
                final_visit_daytime
            from julive_portrait.portrait_lable_summary_user
            where latest_select_city = select_city
            and comjia_unique_id is not null
            and comjia_unique_id_valid_status <> 2
            and select_city is not null
            and substr(product_id,0,3) in ('101','201') 
            and product_id is not null
        )t4
    )t5
    where r = 1
)t2
on t1.user_id = t2.user_id
and t1.select_city = t2.select_city
where split(t1.project_list,'@')[1] is not null;








insert overwrite table julive_app.app_push_frequent
select
    concat_ws('_',id,cast(current_date() as string)) as id,
    '【居理精选】您可能感兴趣的楼盘' title,
    concat('为您推荐【_city_name_】的新房') subtitle,
    city_id,
    concat('comjia://app.comjia.com/h5?data={\"url\":\"https://m.julive.com/_city_name_/project/',project,'.html\",\"recall_id\":\"_recall_id\",\"recall_type\":\"_recall_type\",\"batch\":\"_batch\",\"strategy\":\"_strategy\"}') link
from
(
select
    user_id as id,
    select_city as city_id,
    res as project
from
(
    select
        user_id,
        select_city,
        res,
        row_number() over(partition by user_id order by rand()) r
    from
    (
        select
            user_id,
            select_city,
            fre
        from 
        (
            select
                user_id,
                select_city,
                frequent(select_city,project_list,2) as fre
            from
            (
                select
                    *
                from tmp_etl.tmp_app_push_frequent
            )t6
        )tt
        where fre != ''
    )t7
    lateral view explode(split(fre,',')) num as res
)t8
where r = 1

union all

select
    city_id as id,
    city_id,
    project_id as project
from
(
    select
        city_id,
        project_id,
        row_number() over(partition by city_id order by rand()) r
    from dm.project_rank_leave_phone_es
    lateral view explode(split(project_list,'@')) list as project_id
)t1
where r = 1
)final;








insert overwrite table julive_app.app_push_frequent_hbase
select
    *
from julive_app.app_push_frequent;

