set hive.execution.engine=spark;
set spark.executor.cores=2;
set spark.executor.memory=8g;
set spark.executor.instances=10;
set spark.app.name=app_push_als;
set spark.yarn.queue=etl;
insert overwrite table tmp_etl.temp_app_push_als
select
    *
from julive_app.app_push_als;


WITH
    temp1 AS 
    (
        SELECT
            t1.comjia_unique_id
        FROM
            (SELECT
                regexp_replace(user_id,'comjia_unique_id_','') as comjia_unique_id
            FROM dm.project_rank_his_user_fea_es
            WHERE view_time_le14 > 2000
            OR leave_phone_project_num_le14 > 1 
            )t1
        GROUP BY t1.comjia_unique_id
    ),
    temp2 AS
    (
        SELECT
            comjia_unique_id,
            select_city,
            manufacturer,
            app_version,
            product_id,
            os,
            os_version
        FROM
        (
            SELECT
                comjia_unique_id,
                select_city,
                manufacturer,
                app_version,
                product_id,
                os,
                os_version,
                row_number() over(PARTITION BY comjia_unique_id ORDER BY final_visit_daytime desc) r
            FROM julive_portrait.portrait_lable_summary_user
            WHERE select_city = latest_select_city
            AND comjia_unique_id is not null
            AND comjia_unique_id_valid_status <> 2
            AND select_city is not null
            AND substr(product_id,0,3) in ('101','201') 
            AND product_id is not null
        ) tempr
        WHERE tempr.r = 1
    ),
    temp3 AS
    (
        SELECT
            comjia_unique_id,
            city_id,
            project_id,
            city_id as project_city,
            project_name,
            district_name,
            price_min,
            tag_name
        FROM
        (
            SELECT
                regexp_replace(split(t1.user_id,'@')[0],'comjia_unique_id_','') as comjia_unique_id,
                split(t1.user_id,'@')[1] as city_id,
                t1.project_id,
                t2.city_id as project_city,
                t2.project_name,
                t2.district_name,
                t2.price_min,
                t2.tag_name
            FROM dm.project_rank_als_recommendation t1
            JOIN dwd.dwd_project_portrait_lable t2
            ON t1.project_id = t2.project_id
            WHERE t2.is_publishable = 1
        )temp4
    ),
    temp5 AS
    (
        SELECT
            t1.comjia_unique_id,
            t1.select_city,
            t1.manufacturer,
            t1.app_version,
            t1.product_id,
            t1.os,
            t1.os_version,
            t2.project_id,
            t2.project_city,
            t2.project_name,
            t2.district_name,
            t2.price_min,
            t2.tag_name,
            row_number() over(partition by t1.comjia_unique_id,t1.select_city,t1.manufacturer,t1.app_version,t1.product_id,t1.os,t1.os_version order by rand()) random_r
        FROM
        (
            SELECT
                temp1.comjia_unique_id,
                temp2.select_city,
                temp2.manufacturer,
                temp2.app_version,
                temp2.product_id,
                temp2.os,
                temp2.os_version
            FROM temp1
            JOIN temp2
            ON  temp1.comjia_unique_id = temp2.comjia_unique_id
        )t1
        JOIN temp3 t2
        ON  t1.comjia_unique_id = t2.comjia_unique_id
        AND t1.select_city = t2.city_id
    ),
    temp6 AS
    (
        SELECT
            comjia_unique_id,
            select_city,
            manufacturer,
            app_version,
            product_id,
            os,
            os_version,
            project_id,
            project_city,
            project_name,
            district_name,
            price_min,
            tag_name
        FROM temp5
        WHERE temp5.random_r = 1
    )

insert overwrite table julive_app.app_push_als
SELECT
    comjia_unique_id,
    select_city,
    manufacturer,
    app_version,
    product_id,
    os,
    os_version,
    project_id,
    project_city,
    project_name,
    district_name,
    price_min,
    tag_name
FROM temp6
WHERE project_id is not null;
