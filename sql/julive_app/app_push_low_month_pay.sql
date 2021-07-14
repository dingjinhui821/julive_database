set mapred.job.name=app_push_low_month_pay;
set mapreduce.job.queuename=root.etl;

with tmp as 
(
    select
        house_type_id,
        project_id,
        city_id,
        room_type,
        district_id,
        price,
        down_pay,
        payment_ratio,
        month_pay,
        month_pay_rank_asc
    from
    (
        select
            t1.project_id,
            t1.house_type_id,
            t1.room_type,
            t2.city_id,
            t2.district_id,
            t1.price,
            t1.down_pay,
            t1.payment_ratio,
            t1.month_pay,
            row_number() over(partition by t2.city_id order by t1.month_pay) month_pay_rank_asc
        from
        (
            select
                project_id,
                house_type_id,
                room_type,
                price,
                down_pay,
                payment_ratio,
                month_pay,
                small_family
            from
            (
                select
                    project_id,
                    house_type_id,
                    room_type,
                    price,
                    down_pay,
                    down_pay / price as payment_ratio,
                    month_pay,
                    small_family,
                    row_number() over(partition by project_id order by month_pay) as month_pay_rank
                from ods.cj_house_type
                where month_pay > 0
                and status = 2
            )t4
            where t4.month_pay_rank = 1)t1
        join
        (
            select
                project_id,
                city_id,
                district_id
            from ods.cj_project
            where is_show = 1
            and status = 2
            and project_type = 1
        )t2
        on t1.project_id = t2.project_id
        where t1.small_family = 1
        and t1.payment_ratio <= 0.41
        and t2.district_id NOT IN ('20000032','20000034','20000033','20000031','20000030')
    )t3
    where city_id is not null
),tmp2 as
(
    select
        house_type_id,
        project_id,
        city_id,
        district_id,
        room_type,
        price,
        down_pay,
        payment_ratio,
        month_pay,
        row_number() over(partition by city_id order by rand()) month_pay_rank_rand
    from tmp
    where month_pay_rank_asc <= 10
)


insert overwrite table julive_app.app_push_low_month_pay
select
    house_type_id,
    project_id,
    city_id,
    room_type,
    price,
    down_pay,
    payment_ratio,
    month_pay
from tmp2
where month_pay_rank_rand = 1




















