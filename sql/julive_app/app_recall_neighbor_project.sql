set mapreduce.job.queuename=root.etl;
set hive.execution.engine=spark;
set spark.executor.cores=2;
set spark.executor.memory=10g;
set spark.executor.instances=8;
set spark.app.name=app_recall_neighbor_project;
set spark.yarn.queue=etl;
insert overwrite table julive_app.app_recall_neighbor_project
SELECT
    t1.comjia_unique_id,
    t1.select_city,
    t2.project_name as source_project_name,
    t1.manufacturer,
    t1.app_version,
    t1.product_id,
    t1.os,
    t1.os_version,
    t1.project_id,
    t1.project_city,
    t1.project_name,
    t1.district_name,
    t1.price_min,
    t1.tag_name
FROM
    (SELECT
        portrait.comjia_unique_id,
        portrait.select_city,
        portrait.source_project_id,
        portrait.manufacturer,
        portrait.app_version,
        portrait.product_id,
        portrait.os,
        portrait.os_version,
        portrait.neighbor_project as project_id,
        project.city_id as project_city,
        project.project_name,
        project.district_name,
        project.price_min,
        project.tag_name
    FROM
        (SELECT
            comjia_unique_id,
            select_city,
            source_project_id,
            manufacturer,
            app_version,
            product_id,
            os,
            os_version,
            neighbor_project 
        FROM
            (SELECT
                user.comjia_unique_id,
                user.select_city,
                user.project_id_rank,
                user.project_id as source_project_id,
                user.manufacturer,
                user.app_version,
                user.product_id,
                user.os,
                user.os_version,
                project.neighbor_project,
                row_number() over(PARTITION BY user.comjia_unique_id ORDER BY user.project_id_rank) as pai
            FROM
                (
                SELECT
                    t2.comjia_unique_id,
                    t2.select_city,
                    split(fav_project.col,'_')[0] as project_id,
                    split(fav_project.col,'_')[1] as project_id_rank,
                    t2.manufacturer,
                    t2.app_version,
                    t2.product_id,
                    t2.os,
                    t2.os_version
                FROM
                    (SELECT
                        t1.comjia_unique_id,
                        t1.select_city,
                        t1.fav_project,
                        t1.manufacturer,
                        t1.app_version,
                        t1.product_id,
                        t1.os,
                        t1.os_version
                    FROM
                        (SELECT
                            comjia_unique_id,
                            select_city,
                            fav_project,
                            manufacturer,
                            app_version,
                            product_id,
                            os,
                            os_version,
                            row_number() over(PARTITION BY comjia_unique_id ORDER BY final_visit_daytime DESC) r
                        FROM julive_portrait.portrait_lable_summary_user
                        WHERE 1=1
                        AND fav_project is NOT NULL
                        AND has_subscribe = 0
                        -- 基础条件
                        AND comjia_unique_id is not null
                        AND comjia_unique_id_valid_status <> 2
                        AND select_city is not null
                        AND substr(product_id,0,3) in ('101','201') 
                        AND product_id is not null
                        AND final_visit_daytime <= date_format(date_add(date_format(from_unixtime(unix_timestamp('${hiveconf:dateID}','yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),-6),'yyyy-MM-dd HH:mm:ss')
                        AND final_visit_daytime >= date_format(date_add(date_format(from_unixtime(unix_timestamp('${hiveconf:dateID}','yyyyMMdd'),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),-30),'yyyy-MM-dd HH:mm:ss')
                        AND select_city = latest_select_city)t1
                    WHERE t1.r = 1)t2
                    lateral view explode(split(t2.fav_project,',')) fav_project as col
                ) user
                LEFT JOIN
                (
                SELECT
                    t1.project_id,
                    split(t1.project_ids,',')[0] as neighbor_project
                FROM
                    (SELECT
                        project_id,
                        concat_ws(',',nearby_project_1km,nearby_project_1_2Km,nearby_project_2_3km) as project_ids
                    FROM dwd.dwd_project_portrait_lable)t1
                WHERE t1.project_ids is NOT NULL
                AND t1.project_ids != ''
                )project
            ON user.project_id = project.project_id
            WHERE project.neighbor_project is NOT NULL)final
        WHERE final.pai = 1)portrait
    LEFT JOIN dwd.dwd_project_portrait_lable project
    ON portrait.neighbor_project = project.project_id
    WHERE project.is_show = 1)t1
    LEFT JOIN dwd.dwd_project_portrait_lable t2
    ON t1.source_project_id = t2.project_id
