set mapreduce.job.queuename=root.etl;
set hive.execution.engine=spark;
set spark.executor.cores=2;
set spark.executor.memory=4g;
set spark.executor.instances=4;
set spark.app.name=app_push_low_down_pay;
set spark.yarn.queue=etl;
insert overwrite table julive_app.app_push_low_down_pay
select
    city_id,
    city_name,
    project_id,
    project_name,
    district_name,
    cast(floor(down_pay/10000) as string) down_pay
from
(
    select
        t3.city_id,
        t3.city_name,
        t2.project_id,
        t3.project_name,
        t3.district_name,
        t2.down_pay,
        row_number() over(partition by t3.city_id order by t2.down_pay) as r
    from
    (
        select
            project_id,
            down_pay
        from
        (
            select
                project_id,
                down_pay,
                row_number() over(partition by project_id order by down_pay) r
            from ods.cj_house_type
            where project_type = 1
            and status != 3
            and is_show_room_type = 2
            and is_false = 2
            and down_pay <> 0
        )t1
        where r = 1
    )t2
    join
    (
        select
            city_id,
            city_name,
            project_id,
            project_name,
            district_name
        from dwd.dwd_project_portrait_lable
        where is_publishable = 1
    )t3
    on t2.project_id=t3.project_id
)t4
where t4.r <= 5
