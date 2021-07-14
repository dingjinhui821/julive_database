-- dsp-sem区域报告表

drop table if exists julive_fact.fact_dsp_sem_area_report_dtl;
create table if not exists julive_fact.fact_dsp_sem_area_report_dtl(

dsp_account_id           int               comment '市场投放账户ID',
account_name             string            comment '市场投放账户名',
plan_id                  bigint            comment '推广计划ID',
plan_name                string            comment '推广计划',
media_type               int               comment '媒体类型',
product_type             int               comment '产品形态',
show_num                 int               comment '暂时次数',
click_num                int               comment '点击次数',
bill_cost                double            comment '账面消费',
cost                     double            comment '现金消费',
click_rate               double            comment '点击率',
cpm                      double            comment '千次展示消费',
average_ranking          int               comment '平均排名',
average_click_price      double            comment '平均点击价格',
city                     string            comment '城市',
city_code                int               comment '城市区域码',
province                 string            comment '省',
province_code            int               comment '省区域码',
device                   int               comment '设备类型（0：全部；1：计算机；2：移动设备）',
report_level             int               comment '报告维度 1 账户 2 计划 ',
creator                  int               comment '创建人',
updator                  int               comment '修改人',
create_datetime          string            comment '创建时间',
update_datetime          string            comment '更新时间',
report_date              string            comment '报告日期',
etl_time                 string            comment 'ETL跑数时间'
) comment 'dsp-sem区域报告明细事实表'
partitioned by (pdate string)
stored as parquet 
;



-- 全量同步脚本 

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
regexp_replace(to_date(from_unixtime(report_date)),"-","") as pdate 

from ods.dsp_sem_area_report t1 
where pdate = '20190411'
and to_date(from_unixtime(report_date)) >= '2019-01-01'
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
t1.report_date
;