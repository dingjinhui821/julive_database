

-- --------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------
-- 建表语句 
-- drop table if exists julive_dim.dim_project_info;
-- create table julive_dim.dim_project_info(
-- skey                                              string                  comment '楼盘表代理键:主键',
-- project_id                                        bigint                  comment '楼盘id',
-- project_name                                      string                  comment '楼盘名称',
-- project_num                                       string                  comment '楼盘编号',
-- summary                                           string                  comment '楼盘概述',
-- far                                               string                  comment '容积率',
-- heating                                           string                  comment '供暖方式',
-- car_space                                         string                  comment '车位比', 
-- greening                                          string                  comment '绿化率',
-- developer                                         string                  comment '开发商',
-- address                                           string                  comment '位置描述',
-- ring_road                                         double                  comment '环线',
-- near_distance                                     string                  comment '近环距离',
-- far_distance                                      string                  comment '远环距离',
-- coordinate                                        string                  comment '楼盘百度坐标',
-- ambitus_text                                      string                  comment '周边描述',
-- city_id                                           bigint                  comment '城市id',
-- city_name                                         string                  comment '城市名称',
-- district_id                                       bigint                  comment '区域id',
-- district_name                                     string                  comment '区域名称',
-- project_type                                      bigint                  comment '业态',
-- project_type_desc                                 string                  comment '业态名称',
-- acreage_min                                       double                  comment '最小面积',
-- acreage_max                                       double                  comment '最大面积',
-- price_min                                         double                  comment '最小总价',
-- price_max                                         double                  comment '最大总价',
-- near_subway                                       int                     comment '临地铁',
-- project_status                                    int                     comment '状态，1: 未售  2:在售  3:售罄 4:待售',
-- is_cooperate                                      int                     comment '是否是合作楼盘，1:是  2:否',
-- is_discount                                       int                     comment '是否折扣:1是，2否根据pay_info来判断，有值为1，无值为2',
-- updator                                           bigint                  comment '楼盘维护咨询师',
-- is_outreach                                       int                     comment '是否外联:0不是，1是',
-- lat_lng                                           string                  comment '售楼处经纬度',
-- lng                                               string                  comment '楼盘经度',
-- lat                                               string                  comment '楼盘纬度',
-- city_center_distance                              decimal(9,2)            comment '楼盘距离市中心直线距离(单位：km)',
-- trade_area                                        bigint                  comment '区域板块id',
-- is_loft                                           int                     comment '是否loft:1是，2否',
-- ec_flow                                           int                     comment '是否刷电商,1是,2否',
-- is_show                                           bigint                  comment '楼盘是否显示 1显示 2隐藏',
-- create_date                                       bigint                  comment '创建日期:yyyy-MM-dd',
-- version                                           int                     comment '记录版本号',
-- status                                            int                     comment '当前状态:1 当前数据 0 归档数据',
-- start_date                                        string                  comment '记录生效日期：yyyy-MM-dd',
-- end_date                                          string                  comment '记录结束日期：yyyy-MM-dd',
-- etl_time                                          string                  comment 'ETL跑数日期'
-- ) comment '楼盘维度表' 
-- stored as parquet 
-- ;
-- 




-- --------------------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------------------
-- ETL 


set spark.app.name=dim_project_info;
set mapred.job.name=dim_project_info;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
insert overwrite table julive_dim.dim_project_info 
select 

regexp_replace(uuid(),"-","")                              as skey,
t1.project_id                                              as project_id,
t1.name                                                    as project_name,
t1.project_num                                             as project_num,
t1.summary                                                 as summary,
t1.far                                                     as far,
t1.heating                                                 as heating,
t1.car_space                                               as car_space, 
t1.greening                                                as greening,
t1.developer                                               as developer,
t1.address                                                 as address,
t1.ring_road                                               as ring_road,
t1.near_distance                                           as near_distance,
t1.far_distance                                            as far_distance,
t1.coordinate                                              as coordinate,
t1.ambitus_text                                            as ambitus_text,
t1.city_id                                                 as city_id,
t2.city_name                                               as city_name,
t1.district_id                                             as district_id,
t1.district_name                                           as district_name,
t1.project_type                                            as project_type,
case 
when t1.project_type = 1  then "住宅" 
when t1.project_type = 2  then "别墅" 
when t1.project_type = 3  then "商住" 
when t1.project_type = 55 then "商铺" 
when t1.project_type = 58  then "写字楼" 
when t1.project_type = 59  then "商务型公寓" 

else "其他" end                                            as project_type_desc,
t1.acreage_min                                             as acreage_min,
t1.acreage_max                                             as acreage_max,
t1.price_min                                               as price_min,
t1.price_max                                               as price_max,
t1.near_subway                                             as near_subway,
t1.status                                                  as project_status,
t1.is_cooperate                                            as is_cooperate,
t1.is_discount                                             as is_discount,
t1.updator                                                 as updator,
t1.is_outreach                                             as is_outreach,
t1.lat_lng                                                 as lat_lng,
t1.lng                                                     as lng,
t1.lat                                                     as lat,
default.linear_distance_2_point(t1.lat,t1.lng,t2.lat,t2.lng) as city_center_distance,
t1.trade_area                                              as trade_area,
t1.is_loft                                                 as is_loft,
t1.ec_flow                                                 as ec_flow,
t1.is_show                                                 as is_show,
to_date(from_unixtime(t1.create_datetime))                 as create_date,
1                                                          as version,
1                                                          as status,
to_date(from_unixtime(t1.create_datetime))                 as start_date,
'9999-12-31'                                               as end_date,
current_timestamp()                                        as etl_time 

from ods.cj_project t1 
left join julive_dim.dim_city t2 on t1.city_id = t2.city_id 
;

