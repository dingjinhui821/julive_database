set hive.execution.engine=spark;
set spark.app.name=fact_employee_period_indi;  
set spark.yarn.queue=etl;

--入职日期 所属城市
WITH emp_city_entry as
(
   SELECT
        tmp.employee_id,
        tmp.city_name as city,
        from_unixtime(tmp.create_datetime,'yyyy-MM-dd') as entry_date
        FROM
         (SELECT
             t1.employee_id,
             t1.city_id,
             --if(t2.city_id is null,t1.city_id,t3.city_id) as city_id,
             t3.city_name,
             t1.full_type,
             t1.create_datetime,
             row_number() over(PARTITION BY t1.employee_id ORDER BY t1.create_datetime)rn
    
         FROM ods.hr_manpower_monthly t1
         left join julive_dim.dim_wlmq_city t2 on t1.city_id = t2.city_id
         left join julive_dim.dim_city t3 on t1.city_id = t3.city_id
         WHERE 
         post_name in ('咨询师','咨询主管','咨询经理','城市经理','大区经理','买方咨询师','买方咨询主管','卖方咨询师','卖方咨询主管')
         AND from_unixtime(create_datetime,'yyyy-MM-dd') != '1970-01-01'
         and t1.employee_id is not null
         and t2.city_name is null
         )tmp
         WHERE tmp.rn = 1
),
--转正日期   
emp_full_date as(
SELECT
    t1.employee_id,
    from_unixtime(t1.create_datetime,'yyyy-MM-dd') as full_date
    FROM
    (SELECT
        employee_id,
        create_datetime,
        row_number() over(PARTITION BY employee_id ORDER BY create_datetime)rn
        FROM ods.hr_manpower_monthly
        WHERE full_type = 2)t1
        WHERE t1.rn = 1
),
--离职日期,咨询师学校，毕业时间
emp_school_graduation_leave as
    (select
        t1.employee_id,
        if(t1.school_attributes = '1','985',
        if(t1.school_attributes = '2','211',
        if(t1.school_attributes = '3','一本',
        if(t1.school_attributes = '4','二本',
        if(t1.school_attributes = '5','三本',
        if(t1.school_attributes = '6','专科及以下',
        if(t1.school_attributes = '7','海外院校',t1.school_attributes))))))) as school_attributes,
        from_unixtime(t1.graduation_time,'yyyy-MM-dd') as graduation_date,
        if(from_unixtime(t1.create_datetime,'yyyy-MM-dd') = date_add(current_date(),-1),null,from_unixtime(t1.create_datetime,'yyyy-MM-dd')) as leave_date
        from
        (SELECT
        graduation_time,
        school_attributes,
        employee_id,
        create_datetime,
        row_number() over (partition by employee_id order by create_datetime desc) as rn
        from ods.hr_manpower_monthly
        where post_name in ('咨询师','咨询主管','咨询经理','城市经理','大区经理','买方咨询师','买方咨询主管','卖方咨询师','卖方咨询主管')
        )t1
        where t1.rn=1 and t1.employee_id is not null
    )
INSERT OVERWRITE TABLE julive_fact.fact_employee_period_indi
SELECT
    table1.employee_id,
    table1.city,
    table1.school_attributes,
    table1.entry_date,
    table1.graduation_date,
    table1.batch,
    table1.fresh_graduate,
    table1.leave_date,
    table1.full_date,
    table1.entry_leave_cycle,
    if(table1.full_cycle < 0 ,0,table1.full_cycle) as full_cycle,
    table1.is_full,
    table1.full_on_time,
    table1.30_day_leave,
    table1.60_day_leave,
    table1.90_day_leave,
    table1.180_day_leave,
    if(table2.employee_id is null,0,1) as is_table2,
    if(table2.employee_id is null,null,table2.subscribe_date) as broken_date,
    if(table2.employee_id is null,null,datediff(table2.subscribe_date,table1.entry_date)) as broken_cycle
    FROM
    (SELECT
        tmp1.employee_id,
        tmp1.city,
        tmp1.school_attributes,
        tmp1.entry_date,
        tmp1.graduation_date,
        tmp1.batch,
        tmp1.fresh_graduate,
        tmp1.leave_date,
        tmp2.full_date,
        if(tmp1.leave_date is null,null,datediff(tmp1.leave_date,tmp1.entry_date)) as entry_leave_cycle,
        if(tmp2.full_date is null,null,datediff(tmp2.full_date,tmp1.entry_date)) as full_cycle,
        if(tmp2.full_date is null,0,1) as is_full,
        if(tmp2.full_date is null,null,if(add_months(tmp1.entry_date,4)>tmp2.full_date,1,0)) as full_on_time,
        if(tmp1.leave_date is null,0,if(datediff(tmp1.leave_date,tmp1.entry_date) <= 30,1,0)) as 30_day_leave,
        if(tmp1.leave_date is null,0,if(datediff(tmp1.leave_date,tmp1.entry_date) <= 60,1,0)) as 60_day_leave,
        if(tmp1.leave_date is null,0,if(datediff(tmp1.leave_date,tmp1.entry_date) <= 90,1,0)) as 90_day_leave,
        if(tmp1.leave_date is null,0,if(datediff(tmp1.leave_date,tmp1.entry_date) <= 180,1,0)) as 180_day_leave
        FROM
            (SELECT
                t1.employee_id,
                t1.city,
                t2.school_attributes,
                t1.entry_date,
                t2.graduation_date,
                t2.leave_date,
                concat_ws('-',t1.city,t1.entry_date) as batch,
                if(year(t2.graduation_date) = '1970',0,if(year(t1.entry_date) = year(t2.graduation_date),1,2)) as fresh_graduate
            FROM      emp_city_entry t1
            LEFT JOIN emp_school_graduation_leave t2 on t1.employee_id=t2.employee_id
            )tmp1
            LEFT JOIN emp_full_date tmp2 ON tmp1.employee_id = tmp2.employee_id
        )table1
    LEFT JOIN
        (SELECT
        tmp2.employee_id,
        from_unixtime(tmp2.subscribe_datetime,'yyyy-MM-dd') as subscribe_date
        FROM
            (SELECT
                tmp1.employee_id,
                tmp1.subscribe_datetime,
                tmp1.value,
                row_number() over(PARTITION BY tmp1.employee_id ORDER BY tmp1.subscribe_datetime)rn
                FROM
                (SELECT
                    t4.employee_id,
                    t4.subscribe_datetime,
                    sum(t4.value) as value
                    FROM
                    (SELECT
                        t1.employee_id,
                        t1.subscribe_id,
                        t1.value,
                        t2.subscribe_datetime,
                        t3.entry_date
                        FROM ods.adjust_subscribe_employee_detail t1
                        LEFT JOIN ods.yw_subscribe t2 ON t1.subscribe_id = t2.id
                        LEFT JOIN emp_city_entry t3   ON t1.employee_id = t3.employee_id
                        WHERE from_unixtime(t2.subscribe_datetime,'yyyy-MM-dd') > t3.entry_date)t4
                    GROUP BY
                    t4.employee_id,
                    t4.subscribe_datetime
                )tmp1
            WHERE tmp1.value > 0.2)tmp2
        WHERE tmp2.rn = 1
    )table2 ON table1.employee_id = table2.employee_id
    WHERE table1.employee_id is not null;


