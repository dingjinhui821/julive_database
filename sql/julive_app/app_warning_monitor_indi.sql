--------运行其他程序之前需要运行的代码

set hive.execution.engine=spark;
set spark.app.name=app_warning_wzy_monitor_indi;  
set spark.yarn.queue=etl;                    
set spark.executor.cores=2;                    
set spark.executor.memory=4g;                  
set spark.executor.instances=14;                  
set spark.yarn.executor.memoryOverhead=1024;  
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

--------------插入数据代码

--set etl_date = date_add(current_date(),-3);
set etl_date = concat_ws('-',substr(${hiveconf:etlDate},1,4),substr(${hiveconf:etlDate},5,2),substr(${hiveconf:etlDate},7,2));
----------总通话指标
WITH THSC AS
  (SELECT ztonghua.city_id,
          dim_city.city_name,
          ztonghua.week_range,
          ztonghua.call_dur,
          ztonghua.call_num,
          ztonghua1.call_dur AS call_dur1,
          ztonghua1.call_num AS call_num1
   FROM
     (SELECT city_id,
             week_range,
             sum(t1.call_duration_day)/3600 AS call_dur,
             sum(t1.call_num_day) AS call_num
      FROM dwd.consultant_called_log_clue_report t1
      JOIN julive_dim.dim_date t2 ON t1.call_date = t2.date_str
      WHERE t1.call_date > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据

        AND t1.call_date <= ${hiveconf:etl_date}
      GROUP BY city_id,
               week_range) ztonghua
   LEFT JOIN
     (SELECT city_id,
             avg(if(pindi = "call_dur",tt.current_value,NULL)) AS call_dur,
             avg(if(pindi = "call_num",tt.current_value,NULL)) AS call_num
      FROM julive_app_warning.app_warning_wzy_monitor_indi tt
      WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
        AND pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
        AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
      GROUP BY city_id) ztonghua1 
   ON ztonghua.city_id = ztonghua1.city_id
   LEFT JOIN julive_dim.dim_city
   ON  ztonghua.city_id=dim_city.city_id
)

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT THSC.city_id,
       THSC.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       THSC.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       THSC.call_dur AS current_value,
       NULL AS up_value,
       THSC.call_dur1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "call_dur" AS pindi
FROM THSC
UNION ALL
SELECT THSC.city_id,
       THSC.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       THSC.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       THSC.call_num AS current_value,
       NULL AS up_value,
       THSC.call_num1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "call_num" AS pindi
FROM THSC;


----------------订单当天关闭率（核对无误，出数）
from(
select
d4.city_id,d4.week_range,
count(distinct if(d4.intent=1 and from_unixtime(d4.distribute_datetime,"yyyy-MM-dd")=from_unixtime(d4.intent_low_datetime,"yyyy-MM-dd"),d4.id,null))/count(distinct d4.id) as dangriguanbilv
from
(select
d1.id,d1.distribute_datetime,d1.customer_intent_city as city_id,d3.week_range,d2.intent_low_datetime,d2.intent
from ods.yw_order d1
left join ods.yw_order_require d2 on d1.id=d2.order_id
left join julive_dim.dim_date d3 ON  from_unixtime(d1.distribute_datetime,"yyyy-MM-dd") = d3.date_str
where d1.is_distribute=1 and d1. employee_realname  !='咨询师测试'
) d4
WHERE from_unixtime(d4.distribute_datetime,"yyyy-MM-dd") > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
   and from_unixtime(d4.distribute_datetime,"yyyy-MM-dd") <= ${hiveconf:etl_date}
group by d4.city_id,d4.week_range
) guanbilv
LEFT JOIN
  (SELECT city_id,
          avg(if(pindi='dangriguanbilv',tt.current_value,NULL)) AS dangriguanbilv
   FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
     group by city_id
)  guanbilv1 
ON guanbilv.city_id = guanbilv1.city_id
LEFT JOIN julive_dim.dim_city
   ON  guanbilv.city_id=dim_city.city_id

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT guanbilv.city_id,
       dim_city.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1),
       guanbilv.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       guanbilv.dangriguanbilv AS current_value,
       NULL AS up_value,
       guanbilv1.dangriguanbilv AS down_value,
       ${hiveconf:etl_date} as pdate,
       "dangriguanbilv" as pindi;

-------------------首访相关：当天上户当天首访带看量+当周上户非当天首访带看量+上周上户本周首访带看量（核对无误）
WITH sa AS
  (
SELECT daikan1.city_id,
       dim_city.city_name,
          daikan1.daikan_week_range as week_range,
          daikan1.dangridaikan,
          daikan1.dangzhoudaikan,
          daikan1.shangzhoushanghubenzhoudaikan,
          tdaikan1.city_id AS city_id1,
          tdaikan1.dangridaikan AS dangridaikan1,
          tdaikan1.dangzhoudaikan AS dangzhoudaikan1,
          tdaikan1.shangzhoushanghubenzhoudaikan AS shangzhoushanghubenzhoudaikan1
   FROM
     (SELECT ab.city_id,
             ab.daikan_week_range,
             count(DISTINCT ab.dangridaikan) AS dangridaikan,
             count(DISTINCT ab.dangzhoudaikan) AS dangzhoudaikan,
             count(DISTINCT ab.shangzhoushanghubenzhoudaikan) AS shangzhoushanghubenzhoudaikan
      FROM
        (SELECT tb.id AS order_id,
                from_unixtime(tb.distribute_datetime,"yyyy-MM-dd") AS distribute_time,
                tb.city_id,
                from_unixtime(bb.plan_real_begin_datetime,"yyyy-MM-dd") AS daikan_time,
                bb.id,
                if(from_unixtime(tb.distribute_datetime,"yyyy-MM-dd")= from_unixtime(bb.plan_real_begin_datetime,"yyyy-MM-dd"),bb.id,NULL) AS dangridaikan,
                if(aab.week_range=aabc.week_range
                   AND (from_unixtime(tb.distribute_datetime,"yyyy-MM-dd")!= from_unixtime(bb.plan_real_begin_datetime,"yyyy-MM-dd")),bb.id,NULL) AS dangzhoudaikan,
                if(datediff(date_sub(from_unixtime(bb.plan_real_begin_datetime,"yyyy-MM-dd"),aab.week_id-1),date_sub(from_unixtime(tb.distribute_datetime,"yyyy-MM-dd"),aabc.week_id-1))=7,bb.id,NULL) AS shangzhoushanghubenzhoudaikan,
                aab.week_range AS daikan_week_range,
                aab.week_id AS daikan_week_id,
                aabc.week_range AS shang_week_range
         FROM ods.yw_order tb
         LEFT JOIN
           (SELECT aa.city_id,
                   aa.plan_real_begin_datetime,
                   aa.order_id,
                   aa.id
            FROM
              (SELECT t1. city_id,
                      t1.plan_real_begin_datetime,
                      t1.order_id,
                      t1.id,
                      row_number()over (partition BY t1. order_id
                                        ORDER BY from_unixtime(t1.plan_real_begin_datetime,"yyyy-MM-dd") ASC) AS num
               FROM ods.yw_see_project t1
               WHERE t1. status >=40
                 AND t1. status < 60
                 AND t1. employee_realname <>'咨询师测试' ) aa
            WHERE aa.num=1) bb ON tb.id=bb.order_id
         LEFT JOIN julive_dim.dim_date aab ON from_unixtime(bb.plan_real_begin_datetime,"yyyy-MM-dd")= aab.date_str
         LEFT JOIN julive_dim.dim_date aabc ON from_unixtime(tb.distribute_datetime,"yyyy-MM-dd")= aabc.date_str
         WHERE tb.is_distribute=1 )ab
      WHERE ab.daikan_time > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
        AND ab.daikan_time <= ${hiveconf:etl_date}
      GROUP BY ab.city_id,
               ab.daikan_week_range )daikan1
---------------历史均值
LEFT JOIN
     (SELECT t2.city_id,
             avg(if(pindi='dangridaikan',t2.current_value,NULL)) AS dangridaikan,
             avg(if(pindi='dangzhoudaikan',t2.current_value,NULL)) AS dangzhoudaikan,
             avg(if(pindi='shangzhoushanghubenzhoudaikan',t2.current_value,NULL)) AS shangzhoushanghubenzhoudaikan
      FROM julive_app_warning.app_warning_wzy_monitor_indi t2
      WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
        AND pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
        AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
        GROUP BY city_id
        ) tdaikan1 
ON daikan1.city_id = tdaikan1.city_id
LEFT JOIN julive_dim.dim_city
   ON  daikan1.city_id=dim_city.city_id
)
INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT sa.city_id,
       sa.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       sa.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       sa.dangridaikan AS current_value,
       NULL AS up_value,
       sa.dangridaikan1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "dangridaikan" AS pindi
FROM sa
UNION ALL
SELECT sa.city_id,
       sa.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       sa.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       sa.dangzhoudaikan AS current_value,
       NULL AS up_value,
       sa.dangzhoudaikan1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "dangzhoudaikan" AS pindi
FROM sa
UNION ALL
SELECT sa.city_id,
       sa.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       sa.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       sa.shangzhoushanghubenzhoudaikan AS current_value,
       NULL AS up_value,
       sa.shangzhoushanghubenzhoudaikan1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "shangzhoushanghubenzhoudaikan" AS pindi
FROM sa;

---------------------复访相关：本周首访本周复访带看量+上周首访本周复访带看量（核对无误，出数）
WITH fufangliang AS
  (SELECT    fufang.city_id,
          dim_city.city_name,
          fufang.week_range,
          fufang.dangzhoushoufangzhuanfufang,
          fufang.shangzhoudaikanzhuanfufang,
          fufang1.dangzhoushoufangzhuanfufang AS dangzhoushoufangzhuanfufang1,
          fufang1.shangzhoudaikanzhuanfufang AS shangzhoudaikanzhuanfufang1
   FROM
(SELECT sk5.city_id,
       sk5.week_range,
        count(distinct sk5.dangzhoushoufangzhuanfufang) as dangzhoushoufangzhuanfufang,
        count(distinct sk5.shangzhoudaikanzhuanfufang) as shangzhoudaikanzhuanfufang
FROM
  (SELECT sk4.city_id,
          sk4.week_range,
          sk4.order_id,
          sk4.id,
          sk4.plan_real_begin_datetime,
          sk4.dangzhoushoufangzhuanfufang,
          sk4.shangzhoudaikanzhuanfufang
   FROM
     (SELECT sk1.city_id,
             sk1.order_id,
             sk1.id,
             sk1.plan_real_begin_datetime,
             row_number()over (partition BY sk1.order_id
                               ORDER BY from_unixtime(sk1.plan_real_begin_datetime,"yyyy-MM-dd") ASC) AS rank,
             sk3.firsttime,
             if(sk23.week_range=sk3.first_see_week_range,sk1.id,NULL) AS dangzhoushoufangzhuanfufang,
             if(datediff(date_sub(from_unixtime(sk1.plan_real_begin_datetime,"yyyy-MM-dd"),sk23.week_id-1),date_sub(from_unixtime(sk3.firsttime,"yyyy-MM-dd"),sk3.first_see_week_id-1))=7,sk1.id,NULL) AS shangzhoudaikanzhuanfufang,
             sk23.week_range
      FROM ods.yw_see_project sk1
      LEFT JOIN
        (------第一次带看时间
SELECT ab1.city_id,
       ab1.order_id,
       ab1.id,
       ab1.plan_real_begin_datetime AS firsttime,
       sk24.week_range AS first_see_week_range,
       sk24.week_id AS first_see_week_id
         FROM
           (SELECT sk1.city_id,
                   sk1.order_id,
                   sk1.id,
                   sk1.plan_real_begin_datetime,
                   row_number()over (partition BY sk1.order_id
                                     ORDER BY from_unixtime(sk1.plan_real_begin_datetime,"yyyy-MM-dd") ASC) AS rank
            FROM ods.yw_see_project sk1
            LEFT JOIN ods.yw_order sk2 ON sk1.order_id=sk2.id
            WHERE sk1.status >= 40
              AND sk1.status<60
              AND sk1.employee_realname <>'咨询师测试'
              AND sk2.is_distribute=1 )ab1
         LEFT JOIN julive_dim.dim_date sk24 ON from_unixtime(ab1.plan_real_begin_datetime,"yyyy-MM-dd")=sk24.date_str
         WHERE ab1.rank=1 ) sk3 ON sk1.order_id=sk3.order_id
      LEFT JOIN ods.yw_order sk2 ON sk1.order_id=sk2.id
      LEFT JOIN julive_dim.dim_date sk23 ON from_unixtime(sk1.plan_real_begin_datetime,"yyyy-MM-dd")=sk23.date_str
      WHERE sk1.status >= 40
        AND sk1.status<60
        AND sk1.employee_realname <>'咨询师测试'
        AND sk2.is_distribute=1 )sk4
   WHERE sk4.rank>=2 )sk5
WHERE from_unixtime(sk5.plan_real_begin_datetime,"yyyy-MM-dd")> date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
  AND from_unixtime(sk5.plan_real_begin_datetime,"yyyy-MM-dd") <= ${hiveconf:etl_date}
  GROUP BY sk5.city_id,sk5.week_range) fufang
  ----------------历史均值
LEFT JOIN
     (SELECT city_id,
             avg(if(pindi='dangzhoushoufangzhuanfufang',t2.current_value,NULL)) AS dangzhoushoufangzhuanfufang,
             avg(if(pindi='shangzhoudaikanzhuanfufang',t2.current_value,NULL)) AS shangzhoudaikanzhuanfufang
      FROM julive_app_warning.app_warning_wzy_monitor_indi t2
      WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
        AND pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
        AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
        GROUP BY city_id
        ) fufang1 
ON fufang.city_id = fufang1.city_id
LEFT JOIN julive_dim.dim_city
   ON  fufang.city_id=dim_city.city_id
)
INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT fufangliang.city_id,
       fufangliang.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1),
       fufangliang.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       fufangliang.dangzhoushoufangzhuanfufang AS current_value,
       NULL AS up_value,
       fufangliang.dangzhoushoufangzhuanfufang1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "dangzhoushoufangzhuanfufang" AS pindi
FROM fufangliang
UNION ALL
SELECT fufangliang.city_id,
       fufangliang.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1),
       fufangliang.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       fufangliang.shangzhoudaikanzhuanfufang AS current_value,
       NULL AS up_value,
       fufangliang.shangzhoudaikanzhuanfufang1 AS down_value,
       ${hiveconf:etl_date} AS pdate,
       "shangzhoudaikanzhuanfufang" AS pindi
FROM fufangliang;

---------------------人均上户量（核对无误，出数）
with shanghuliang as(
select
renjunshanghu.city_id,dim_city.city_name,renjunshanghu.week_range,renjunshanghu.zhuanzhengrenjunshanghu,renjunshanghu.weizhuanzhengrenjunshanghu,
renjunshanghu1.zhuanzhengrenjunshanghu as zhuanzhengrenjunshanghu1,renjunshanghu1.weizhuanzhengrenjunshanghu as weizhuanzhengrenjunshanghu1
from(
SELECT hd7.city_id,
       hd8.week_range,
       sum(if(hd7.full_type=2,hd7.shanghuliang,0))/count(DISTINCT if(hd7.full_type=2,hd7.zixunshizhong,NULL)) AS zhuanzhengrenjunshanghu,
       sum(if(hd7.full_type=1,hd7.shanghuliang,0))/count(DISTINCT if(hd7.full_type=1,hd7.zixunshizhong,NULL)) AS weizhuanzhengrenjunshanghu
FROM
  (SELECT hd6.distribute_datetime,
          hd6.city_id,
          hd6.zixunshizhong,
          hd6.shanghuliang,
          hr2.full_type
   FROM
     (SELECT from_unixtime(hd5.distribute_datetime,"yyyy-MM-dd") AS distribute_datetime,
             hd5.city_id,
             hd5.zixunshizhong,
             count(DISTINCT hd5.id) AS shanghuliang
      FROM
        (SELECT hd4.id,
                hd4.employee_id,
                hd4.employee_realname,
                hd4.history_employee_id,
                hd4.history_employee_realname,
                hd4.create_datetime,
                hd4.distribute_datetime,
                hd4.order_id01,
                hd4.create_datetime01,
                hd4.distribute_datetime01,
                hd4.zixunshijilu,
                hd4.zixunshi01,
                if(hd4.zixunshijilu IS NULL
                   OR hd4.zixunshijilu='',hd4.zixunshi01,hd4.zixunshijilu) AS zixunshizhong,
                hd4.city_id
         FROM
           (SELECT hd3.id,
                   hd3.employee_id,
                   hd3.employee_realname,
                   hd3.history_employee_id,
                   hd3.history_employee_realname,
                   hd3.city_id,
                   hd3.create_datetime,
                   hd3.distribute_datetime,
                   hd2.alloc_datetime AS distribute_datetime01,
                   hd2.order_id AS order_id01,
                   hd2.create_datetime AS create_datetime01,
                   hd2.zixunshi AS zixunshijilu,
                   if(hd3.history_employee_id IS NULL
                      OR hd3.history_employee_id='',hd3.employee_id,hd3.history_employee_id) AS zixunshi01
            FROM ods.yw_order hd3
            LEFT JOIN
              (SELECT ff1.order_id,
                      ff1.employee_id_now,
                      ff1.employee_id_old,
                      ff1.alloc_datetime,
                      ff1.create_datetime,
                      ff1.update_datetime,
                      ff1.zixunshi,
                      ff1.rank
               FROM
                 (SELECT hd1.order_id,
                         hd1.employee_id_now,
                         hd1.employee_id_old,
                         hd1.alloc_datetime,
                         hd1.create_datetime,
                         hd1.update_datetime,
                         if(hd1.employee_id_old IS NULL
                            OR hd1.employee_id_old=''
                            OR hd1.employee_id_old=0,hd1.employee_id_now,hd1.employee_id_old) AS zixunshi,
                         row_number() over(partition BY hd1.order_id
                                           ORDER BY from_unixtime(hd1.create_datetime) ASC) AS rank
                  FROM ods.yw_order_alloc_record hd1)ff1
               WHERE ff1.rank=1 )hd2 ON hd3.id=hd2.order_id
            WHERE hd3.is_distribute=1 ) hd4)hd5
      WHERE hd5.zixunshizhong IS NOT NULL
        OR hd5.zixunshizhong != ''
      GROUP BY from_unixtime(hd5.distribute_datetime,"yyyy-MM-dd"),
               hd5.city_id,
               hd5.zixunshizhong)hd6
   LEFT JOIN
     (SELECT hr1.create_datetime,
             hr1.employee_id,
             hr1.employee_name,
             hr1.full_type
      FROM
        (SELECT hr.create_datetime,
                hr.employee_name,
                hr.full_type,
                hr.employee_id,
                row_number() over(partition BY hr.employee_id,from_unixtime(hr.create_datetime,"yyyy-MM-dd")
                                  ORDER BY 1) AS rank
         FROM ods.hr_manpower_monthly hr) hr1
      WHERE hr1.rank=1)hr2 ON hd6.zixunshizhong=hr2.employee_id
   AND hd6.distribute_datetime=from_unixtime(hr2.create_datetime,"yyyy-MM-dd")) hd7
JOIN julive_dim.dim_date hd8 ON hd7.distribute_datetime= hd8.date_str
WHERE hd7.distribute_datetime> date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据据
AND hd7.distribute_datetime <= ${hiveconf:etl_date}
GROUP BY hd7.city_id,
         hd8.week_range
)renjunshanghu
----------------历史均值
LEFT JOIN
  (SELECT city_id,
          avg(if(pindi='zhuanzhengrenjunshanghu',t2.current_value,NULL)) AS zhuanzhengrenjunshanghu,
          avg(if(pindi='weizhuanzhengrenjunshanghu',t2.current_value,NULL)) AS weizhuanzhengrenjunshanghu
   FROM julive_app_warning.app_warning_wzy_monitor_indi t2
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
group by city_id
) renjunshanghu1 
ON renjunshanghu.city_id = renjunshanghu1.city_id
LEFT JOIN julive_dim.dim_city
   ON  renjunshanghu.city_id=dim_city.city_id
)

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT shanghuliang.city_id,
       shanghuliang.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
      shanghuliang.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       shanghuliang.zhuanzhengrenjunshanghu AS current_value,
       NULL AS up_value,
       shanghuliang .zhuanzhengrenjunshanghu1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "zhuanzhengrenjunshanghu" as pindi
from shanghuliang
union all
SELECT shanghuliang.city_id,
       shanghuliang.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       shanghuliang.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
      shanghuliang.weizhuanzhengrenjunshanghu AS current_value,
       NULL AS up_value,
       shanghuliang.weizhuanzhengrenjunshanghu1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "weizhuanzhengrenjunshanghu" as pindi
from shanghuliang ;


------------------------接上户人数（核对无误，出数）
FROM
    (
SELECT hd7.city_id,
       hd8.week_range,
       count(distinct hd7.zixunshizhong) as employee_num 
FROM
  (SELECT hd6.distribute_datetime,
          hd6.city_id,
          hd6.zixunshizhong,
          hd6.shanghuliang,
          hr2.full_type
   FROM
     (SELECT from_unixtime(hd5.distribute_datetime,"yyyy-MM-dd") AS distribute_datetime,
             hd5.city_id,
             hd5.zixunshizhong,
             count(DISTINCT hd5.id) AS shanghuliang
      FROM
        (SELECT hd4.id,
                hd4.employee_id,
                hd4.employee_realname,
                hd4.history_employee_id,
                hd4.history_employee_realname,
                hd4.create_datetime,
                hd4.distribute_datetime,
                hd4.order_id01,
                hd4.create_datetime01,
                hd4.distribute_datetime01,
                hd4.zixunshijilu,
                hd4.zixunshi01,
                if(hd4.zixunshijilu IS NULL
                   OR hd4.zixunshijilu='',hd4.zixunshi01,hd4.zixunshijilu) AS zixunshizhong,
                hd4.city_id
         FROM
           (SELECT hd3.id,
                   hd3.employee_id,
                   hd3.employee_realname,
                   hd3.history_employee_id,
                   hd3.history_employee_realname,
                   hd3.city_id,
                   hd3.create_datetime,
                   hd3.distribute_datetime,
                   hd2.alloc_datetime AS distribute_datetime01,
                   hd2.order_id AS order_id01,
                   hd2.create_datetime AS create_datetime01,
                   hd2.zixunshi AS zixunshijilu,
                   if(hd3.history_employee_id IS NULL
                      OR hd3.history_employee_id='',hd3.employee_id,hd3.history_employee_id) AS zixunshi01
            FROM ods.yw_order hd3
            LEFT JOIN
              (SELECT ff1.order_id,
                      ff1.employee_id_now,
                      ff1.employee_id_old,
                      ff1.alloc_datetime,
                      ff1.create_datetime,
                      ff1.update_datetime,
                      ff1.zixunshi,
                      ff1.rank
               FROM
                 (SELECT hd1.order_id,
                         hd1.employee_id_now,
                         hd1.employee_id_old,
                         hd1.alloc_datetime,
                         hd1.create_datetime,
                         hd1.update_datetime,
                         if(hd1.employee_id_old IS NULL
                            OR hd1.employee_id_old=''
                            OR hd1.employee_id_old=0,hd1.employee_id_now,hd1.employee_id_old) AS zixunshi,
                         row_number() over(partition BY hd1.order_id
                                           ORDER BY from_unixtime(hd1.create_datetime) ASC) AS rank
                  FROM ods.yw_order_alloc_record hd1)ff1
               WHERE ff1.rank=1 )hd2 ON hd3.id=hd2.order_id
            WHERE hd3.is_distribute=1 ) hd4)hd5
      WHERE hd5.zixunshizhong IS NOT NULL
        OR hd5.zixunshizhong != ''
      GROUP BY from_unixtime(hd5.distribute_datetime,"yyyy-MM-dd"),
               hd5.city_id,
               hd5.zixunshizhong)hd6
   LEFT JOIN
     (SELECT hr1.create_datetime,
             hr1.employee_id,
             hr1.employee_name,
             hr1.full_type
      FROM
        (SELECT hr.create_datetime,
                hr.employee_name,
                hr.full_type,
                hr.employee_id,
                row_number() over(partition BY hr.employee_id,from_unixtime(hr.create_datetime,"yyyy-MM-dd")
                                  ORDER BY 1) AS rank
         FROM ods.hr_manpower_monthly hr) hr1
      WHERE hr1.rank=1)hr2 ON hd6.zixunshizhong=hr2.employee_id
   AND hd6.distribute_datetime=from_unixtime(hr2.create_datetime,"yyyy-MM-dd")) hd7
JOIN julive_dim.dim_date hd8 ON hd7.distribute_datetime= hd8.date_str
WHERE hd7.distribute_datetime> date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据据
AND hd7.distribute_datetime <= ${hiveconf:etl_date}
GROUP BY hd7.city_id,
         hd8.week_range
) renshu
LEFT JOIN
  (SELECT city_id,
          avg(if(pindi = "jieshanghu_rs",tt.current_value,NULL)) AS employee_num
   FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
   GROUP BY city_id ) renshu1
ON renshu.city_id = renshu1.city_id
 LEFT JOIN julive_dim.dim_city
   ON  renshu.city_id=dim_city.city_id
                      
INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT renshu.city_id,
       dim_city.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       renshu.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       renshu.employee_num AS current_value,
       NULL AS up_value,
       renshu1.employee_num AS down_value,
       ${hiveconf:etl_date} as pdate,
       'jieshanghu_rs' as pindi;


---------------------------------首复访相关通话时长（核对无误,出数）
with zzth as(
select
tonghua.city_id,dim_city.city_name,tonghua.week_range,tonghua.dangtianshanghudangtiandanjuntonghua_min,tonghua.dangzhoushanghufeidangtiandanjuntonghua_min,tonghua.shangzhoushanghubenzhoudanjuntonghua_min,
tonghua.benzhoushoufanghoudanjuntonghua_min,tonghua.shangzhoushoufangbenzhoudanjunshichang_min,
tonghua1.dangtianshanghudangtiandanjuntonghua_min as dangtianshanghudangtiandanjuntonghua_min1,
tonghua1.dangzhoushanghufeidangtiandanjuntonghua_min as dangzhoushanghufeidangtiandanjuntonghua_min1,
tonghua1.shangzhoushanghubenzhoudanjuntonghua_min as shangzhoushanghubenzhoudanjuntonghua_min1,
tonghua1.benzhoushoufanghoudanjuntonghua_min as benzhoushoufanghoudanjuntonghua_min1,
tonghua1.shangzhoushoufangbenzhoudanjunshichang_min as shangzhoushoufangbenzhoudanjunshichang_min1
from
(SELECT tm7.city_id,
       tm7.tonghua_week_range AS week_range,
       sum(if(to_date(tm7.release_time)=from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"),tm7.call_duration/60,0))/count(DISTINCT if(to_date(tm7.release_time)=from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"),tm7.order_id,NULL)) AS dangtianshanghudangtiandanjuntonghua_min,
       sum(if((datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))<=0
               OR tm7.first_see_time IS NULL
               OR tm7.first_see_time='')
              AND tm7.tonghua_week_range=tm7.shanghu_week_range
              AND datediff(to_date(tm7.release_time),from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"))>0,tm7.call_duration/60,0))/COUNT (DISTINCT if((datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))<=0
                                                                                                                                                         OR tm7.first_see_time IS NULL
                                                                                                                                                         OR tm7.first_see_time='')
                                                                                                                                                        AND tm7.tonghua_week_range=tm7.shanghu_week_range
                                                                                                                                                        AND datediff(to_date(tm7.release_time),from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"))>0,tm7.order_id,NULL)) AS dangzhoushanghufeidangtiandanjuntonghua_min,
       sum(if((datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))<=0 OR tm7.first_see_time IS NULL OR tm7.first_see_time='')
                                    AND datediff(date_sub(to_date(tm7.release_time),tm7.tonghua_week_id-1),date_sub(from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"),tm7.shanghu_week_id-1))=7,tm7.call_duration/60,0))/count(DISTINCT if((datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))<=0
                                    OR tm7.first_see_time IS NULL
                                    OR tm7.first_see_time='')
                                    AND datediff(date_sub(to_date(tm7.release_time),tm7.tonghua_week_id-1),date_sub(from_unixtime(tm7.distribute_datetime,"yyyy-MM-dd"),tm7.shanghu_week_id-1))=7,tm7.order_id,0)) AS shangzhoushanghubenzhoudanjuntonghua_min,
         sum(if(datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))>0
                                    AND tm7.tonghua_week_range=tm7.first_see_week_range,tm7.call_duration/60,0))/count(DISTINCT if(datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))>0
                                    AND tm7.tonghua_week_range=tm7.first_see_week_range,tm7.order_id,NULL)) AS benzhoushoufanghoudanjuntonghua_min,   
       
    sum(if(from_unixtime(tm7.first_see_time,"yyyy-MM-dd") > '2010-01-01'
                                                                               AND datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))>0
                                                                                 AND datediff(date_sub(to_date(tm7.release_time),tm7.tonghua_week_id-1),date_sub(from_unixtime(tm7.first_see_time,"yyyy-MM-dd"),tm7.first_see_week_id-1))=7,tm7.call_duration/60,0)) /count(DISTINCT if(from_unixtime(tm7.first_see_time,"yyyy-MM-dd") > '2010-01-01'
                                                                                AND datediff(to_date(tm7.release_time),from_unixtime(tm7.first_see_time,"yyyy-MM-dd"))>0
                                                                                AND datediff(date_sub(to_date(tm7.release_time),tm7.tonghua_week_id-1),date_sub(from_unixtime(tm7.first_see_time,"yyyy-MM-dd"),tm7.first_see_week_id-1))=7,tm7.order_id,NULL)) AS shangzhoushoufangbenzhoudanjunshichang_min
FROM  (--------单次通话7200秒以内的通话记录
SELECT tm4.release_time,
       tm4.order_id,
       tm4.id,
       tm6.distribute_datetime,
       tm6.city_id,
       tm5.first_see_time,
       tm5.first_see_week_range,
       tm5.first_see_week_id,
       tmm2.week_range AS tonghua_week_range,
       tmm2.week_id AS tonghua_week_id,
       tm4.call_duration,
       tmm3.week_range AS shanghu_week_range,
       tmm3.week_id AS shanghu_week_id
   FROM ods.yw_sys_number_talking tm4
   LEFT JOIN ods.yw_order tm6 ON tm4.order_id=tm6.id
   LEFT JOIN
     (SELECT tm3.city_id,
             tm3.order_id,
             tm3.plan_real_begin_datetime AS first_see_time,
             tmm1.week_range AS first_see_week_range,
             tmm1.week_id AS first_see_week_id
      FROM
        (SELECT tm1.city_id,
                tm1.order_id,
                tm1.id,
                tm1.plan_real_begin_datetime,
                row_number()over (partition BY tm1.order_id
                                  ORDER BY from_unixtime(tm1.plan_real_begin_datetime,"yyyy-MM-dd") ASC) AS rank
         FROM ods.yw_see_project tm1
         LEFT JOIN ods.yw_order tm2 ON tm1.order_id=tm2.id
         WHERE tm1.status >= 40
           AND tm1.status<60
           AND tm1.employee_realname <>'咨询师测试'
           AND tm2.is_distribute=1 )tm3
      LEFT JOIN julive_dim.dim_date tmm1 ON from_unixtime(tm3.plan_real_begin_datetime,"yyyy-MM-dd")=tmm1.date_str
      WHERE tm3.rank=1 )tm5 ON tm4.order_id=tm5.order_id
   LEFT JOIN julive_dim.dim_date tmm2 ON to_date(tm4.release_time)=tmm2.date_str
   LEFT JOIN julive_dim.dim_date tmm3 ON from_unixtime(tm6.distribute_datetime,"yyyy-MM-dd")=tmm3.date_str
   WHERE tm4.order_id>0
     AND tm4.call_duration<=7200
     AND tm4.call_result='ANSWERED'
     AND tm6.is_distribute=1 )tm7
WHERE to_date(tm7.release_time) > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
  AND to_date(tm7.release_time) <= ${hiveconf:etl_date}
group by tm7.city_id,
       tm7.tonghua_week_range
)tonghua


LEFT JOIN
  (SELECT city_id,
          avg(if(pindi = "dangtianshanghudangtiandanjuntonghua_min",tt.current_value,NULL)) AS dangtianshanghudangtiandanjuntonghua_min,
          avg(if(pindi = "dangzhoushanghufeidangtiandanjuntonghua_min",tt.current_value,NULL)) AS dangzhoushanghufeidangtiandanjuntonghua_min,
          avg(if(pindi = "shangzhoushanghubenzhoudanjuntonghua_min",tt.current_value,NULL)) AS shangzhoushanghubenzhoudanjuntonghua_min,
          avg(if(pindi = "benzhoushoufanghoudanjuntonghua_min",tt.current_value,NULL)) AS benzhoushoufanghoudanjuntonghua_min,
          avg(if(pindi = "shangzhoushoufangbenzhoudanjunshichang_min",tt.current_value,NULL)) AS shangzhoushoufangbenzhoudanjunshichang_min
   FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
   GROUP BY city_id ) tonghua1
ON tonghua.city_id = tonghua1.city_id
LEFT JOIN julive_dim.dim_city
ON  tonghua.city_id=dim_city.city_id
)

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT zzth.city_id,
       zzth.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       zzth.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       zzth.dangtianshanghudangtiandanjuntonghua_min AS current_value,
       NULL AS up_value,
       zzth.dangtianshanghudangtiandanjuntonghua_min1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "dangtianshanghudangtiandanjuntonghua_min" as pindi
FROM zzth
union all
SELECT zzth.city_id,
       zzth.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       zzth.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       zzth.dangzhoushanghufeidangtiandanjuntonghua_min AS current_value,
       NULL AS up_value,
       zzth.dangzhoushanghufeidangtiandanjuntonghua_min1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "dangzhoushanghufeidangtiandanjuntonghua_min" as pindi
FROM zzth
union all
SELECT zzth.city_id,
       zzth.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       zzth.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       zzth.shangzhoushanghubenzhoudanjuntonghua_min AS current_value,
       NULL AS up_value,
       zzth.shangzhoushanghubenzhoudanjuntonghua_min1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "shangzhoushanghubenzhoudanjuntonghua_min" as pindi
FROM zzth
union all
SELECT zzth.city_id,
       zzth.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       zzth.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       zzth.benzhoushoufanghoudanjuntonghua_min AS current_value,
       NULL AS up_value,
       zzth.benzhoushoufanghoudanjuntonghua_min1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "benzhoushoufanghoudanjuntonghua_min" as pindi
FROM zzth
union all
SELECT zzth.city_id,
       zzth.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       zzth.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       zzth.shangzhoushoufangbenzhoudanjunshichang_min AS current_value,
       NULL AS up_value,
       zzth.shangzhoushoufangbenzhoudanjunshichang_min1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "shangzhoushoufangbenzhoudanjunshichang_min" as pindi
FROM zzth;

-------------------上户-带看-认购指标（核对无误，出数）
WITH ZL AS
(
SELECT  zongliang.city_id,
        dim_city.city_name,
        zongliang.week_range,
        zongliang.shanghu_num,
        zongliang.daikan_num,
        zongliang.rengou_num,
        zongliang1.shanghu_num as shanghu_num1,
        zongliang1.daikan_num as daikan_num1,
        zongliang1.rengou_num as rengou_num1
FROM 
(SELECT shanghuliang.city_id,
        shanghuliang.week_range,
        coalesce(shanghu_num,0) as shanghu_num,
         coalesce(daikan_num,0) as daikan_num,
         coalesce(rengou_num,0) as rengou_num
 FROM
  (SELECT customer_intent_city as city_id,
          week_range,
          count(DISTINCT t1.id) AS shanghu_num
   FROM ods.yw_order t1
   JOIN julive_dim.dim_date t2 ON from_unixtime(t1.distribute_datetime,"yyyy-MM-dd")= t2.date_str
   WHERE t1.is_distribute=1
     AND from_unixtime(t1.distribute_datetime,"yyyy-MM-dd") > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据

     AND from_unixtime(t1.distribute_datetime,"yyyy-MM-dd") <= ${hiveconf:etl_date}
   GROUP BY t1.customer_intent_city,
            t2.week_range
   ) shanghuliang
  LEFT JOIN
   (SELECT t1.project_city_id as city_id, t2.week_range, count(DISTINCT t1.see_id) AS daikan_num
   FROM julive_fact.fact_see_project_dtl t1
   JOIN julive_dim.dim_date t2 ON to_date(t1.plan_real_begin_time)= t2.date_str
   WHERE t1. status >=40
     AND t1. status < 60
     AND t1. clue_emp_realname <>'咨询师测试'
     AND to_date(t1.plan_real_begin_time)> date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
        and to_date(t1.plan_real_begin_time)<= ${hiveconf:etl_date}
    GROUP BY t1.project_city_id,t2.week_range
    ) daikan
   ON shanghuliang.city_id=daikan.city_id AND shanghuliang.week_range=daikan.week_range
   LEFT JOIN
   (SELECT a.project_city_id as city_id, aa.week_range, count(DISTINCT a.subscribe_id) AS rengou_num
   FROM julive_fact.fact_subscribe_dtl a
   JOIN julive_dim.dim_date aa ON to_date(subscribe_time)= aa.date_str
   WHERE (a.subscribe_status=1
          OR a.subscribe_status=2)
     AND (a.subscribe_type =1
          OR a.subscribe_type =4)
     AND to_date(subscribe_time)> date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
     and to_date(subscribe_time)<= ${hiveconf:etl_date}
     GROUP BY a.project_city_id,aa.week_range
    ) rengou
    ON shanghuliang.city_id=rengou.city_id AND shanghuliang.week_range=rengou.week_range
 )zongliang
LEFT JOIN 
 (
  SELECT city_id,
       avg(if(pindi='shanghu_num',tt.current_value,NULL)) AS shanghu_num,
       avg(if(pindi='daikan_num',tt.current_value,NULL)) AS daikan_num,
       avg(if(pindi='rengou_num',tt.current_value,NULL)) AS rengou_num
  FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
     group by city_id
    ) zongliang1
ON zongliang.city_id = zongliang1.city_id
LEFT JOIN julive_dim.dim_city
ON  zongliang.city_id=dim_city.city_id
)

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT ZL.city_id,
       ZL.city_name AS city_name,
         if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
         ZL.week_range,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         ZL.shanghu_num AS current_value,
         NULL AS up_value,
         ZL.shanghu_num1 AS down_value,
         ${hiveconf:etl_date} AS pdate,
         "shanghu_num" AS pindi
from ZL
         
UNION ALL
SELECT ZL.city_id,
        ZL.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       ZL.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       ZL.daikan_num AS current_value,
       NULL AS up_value,
       ZL.daikan_num1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "daikan_num" as pindi
FROM ZL
       
UNION ALL
SELECT ZL.city_id,
        ZL.city_name AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) AS week_id,
       ZL.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       ZL.rengou_num AS current_value,
       NULL AS up_value,
       ZL.rengou_num1 AS down_value,
       ${hiveconf:etl_date} as pdate,
       "rengou_num" as pindi
FROM ZL;

----------上户量集团
FROM
  (SELECT  week_range,
          count(DISTINCT t1.id) AS jituanshanghu_num,
          "1" as city_id
   FROM ods.yw_order t1
   JOIN julive_dim.dim_date t2 ON from_unixtime(t1.distribute_datetime,"yyyy-MM-dd")= t2.date_str
   WHERE t1.is_distribute=1
     AND from_unixtime(t1.distribute_datetime,"yyyy-MM-dd") > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据

     AND from_unixtime(t1.distribute_datetime,"yyyy-MM-dd") <= ${hiveconf:etl_date}
   GROUP BY t2.week_range) shanghuliang
LEFT JOIN (
SELECT city_id,
       avg(if(pindi='jituanshanghu_num',tt.current_value,NULL)) AS jituanshanghu_num
FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
     AND city_id="1"
     group by city_id
    ) shanghuliang1 ON shanghuliang.city_id = shanghuliang1.city_id
  INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
  SELECT shanghuliang.city_id,
         "集团" AS city_name,
         if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
         shanghuliang.week_range,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         shanghuliang.jituanshanghu_num AS current_value,
         NULL AS up_value,
         shanghuliang1.jituanshanghu_num AS down_value,
         ${hiveconf:etl_date} AS pdate,
         "jituanshanghu_num" AS pindi;


-------------------带看集团
from
  (SELECT t2.week_range, 
      sum(t1.see_num) AS jituandaikan_num,
     "1" as city_id
   FROM julive_fact.fact_see_project_dtl t1
   JOIN julive_dim.dim_date t2 ON to_date(t1.plan_real_begin_time)= t2.date_str
   WHERE t1. status >=40
     AND t1. status < 60
     AND t1. clue_emp_realname <>'咨询师测试'
     AND to_date(t1.plan_real_begin_time) > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
        and to_date(t1.plan_real_begin_time) <= ${hiveconf:etl_date}
GROUP BY week_range) daikan
LEFT JOIN
  (SELECT city_id,
          avg(if(pindi='jituandaikan_num',tt.current_value,NULL)) AS jituandaikan_num
   FROM julive_app_warning.app_warning_wzy_monitor_indi tt
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
     and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
     AND city_id="1"
     group by city_id
)  daikan1 ON daikan.city_id = daikan1.city_id
INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT daikan.city_id,
       "集团"  AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       daikan.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       daikan.jituandaikan_num AS current_value,
       NULL AS up_value,
       daikan1.jituandaikan_num AS down_value,
       ${hiveconf:etl_date} as pdate,
       "jituandaikan_num" as pindi;

-------------------认购量集团
from
  (SELECT 
       aa.week_range, 
      count(DISTINCT a.subscribe_id) AS jituanrengou_num,
      "1" as city_id
   FROM julive_fact.fact_subscribe_dtl a
   JOIN julive_dim.dim_date aa ON to_date(subscribe_time)= aa.date_str
   WHERE (a.subscribe_status=1
          OR a.subscribe_status=2)
     AND (a.subscribe_type =1
          OR a.subscribe_type =4)
     AND to_date(subscribe_time) > date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))) -- 取本周数据
     and to_date(subscribe_time)<= ${hiveconf:etl_date}
GROUP BY aa.week_range) rengou
LEFT JOIN
  (SELECT city_id,
          avg(if(pindi='jituanrengou_num',t2.current_value,NULL)) AS jituanrengou_num
   FROM julive_app_warning.app_warning_wzy_monitor_indi t2
   WHERE pdate > date_add(${hiveconf:etl_date},-(84+(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1))))
        and pdate <= date_add(${hiveconf:etl_date},-(if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)))
     AND week_id = if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1)
      AND city_id="1"
group by city_id
) rengou1 ON rengou.city_id = rengou1.city_id

INSERT overwrite TABLE julive_app_warning.app_warning_wzy_monitor_indi partition(pdate,pindi)
SELECT rengou.city_id,
      "集团" AS city_name,
       if(dayofweek(${hiveconf:etl_date}) = 1,7,dayofweek(${hiveconf:etl_date})-1) as week_id,
       rengou.week_range,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       rengou.jituanrengou_num AS current_value,
       NULL AS up_value,
       rengou1.jituanrengou_num AS down_value,
       ${hiveconf:etl_date} as pdate,
       "jituanrengou_num" as pindi;
