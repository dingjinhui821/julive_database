-- dsp-sem区域报告表

set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 取昨天日期 
set spark.app.name=fact_dsp_sem_area_report_dtl;
set mapred.job.name=fact_dsp_sem_area_report_dtl;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert overwrite table julive_fact.fact_dsp_sem_area_report_dtl partition(pdate)

select 

t1.dsp_account_id, -- 市场投放账户ID
t1.account_name, -- 市场投放账户名
t1.plan_id, -- 推广计划ID
t1.plan_name, -- 推广计划
t1.media_type, -- 媒体类型
t1.product_type, -- 产品形态
t1.show_num, -- 暂时次数
t1.click_num, -- 点击次数
t1.bill_cost, -- 账面消费
t1.cost, -- 现金消费
t1.click_rate, -- 点击率
t1.cpm, -- 千次展示消费
t1.average_ranking, -- 平均排名
t1.average_click_price, -- 平均点击价格
t1.city, -- 城市
t1.city_code, -- 城市区域码
t1.province, -- 省
t1.province_code, -- 省区域码
t1.device, -- 设备类型（0：全部；1：计算机；2：移动设备）
t1.report_level, -- 报告维度 1 账户 2 计划  
t1.creator, -- 创建人
t1.updator, -- 修改人
from_unixtime(max(t1.create_datetime),"yyyy-MM-dd HH:mm:ss") as create_datetime,
from_unixtime(max(t1.update_datetime),"yyyy-MM-dd HH:mm:ss") as update_datetime,
from_unixtime(t1.report_date,"yyyy-MM-dd") as report_date,
current_timestamp() as etl_time,
t1.pdate as pdate 

from ods.dsp_sem_area_report t1 
where t1.pdate = ${hiveconf:etl_date}
group by 
t1.dsp_account_id,
t1.account_name,
t1.media_type,
t1.product_type,
t1.plan_name,
t1.plan_id,
t1.show_num,
t1.click_num,
t1.bill_cost,
t1.cost,
t1.click_rate,
t1.cpm,
t1.average_ranking,
t1.average_click_price,
t1.city,
t1.city_code,
t1.province,
t1.province_code,
t1.device,
t1.report_level,
t1.creator,
t1.updator,
t1.report_date,
t1.pdate
;
