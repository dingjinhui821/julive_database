-- 渠道维度表ETL

set etl_date = '${hiveconf:etlDate}';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); 

set spark.app.name=dim_channel_info;
set mapred.job.name=dim_channel_info;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
--  功能描述：渠道纬度表,每天全量重跑                             
--  输 入 表：
--        ods.cj_channel                                 -- 渠道表
--        ods.dim_city                                 	 -- 城市纬度表        
--        ods.cj_agency                                  -- app包信息
--
--  输 出 表：julive_dim.dim_channel_info 
--
--  创 建 者：  未知  186xxx  xxx@julive.com
--  创建日期： xxx/08/07 14:16
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--  20210524  姜宝桥  新增表：ods.cj_agency，修复app_type_name属性

insert overwrite table julive_dim.dim_channel_info 
select 
	yy.channel_id as skey, -- 代理键
	yy.channel_id,
	yy.channel_name,
	yy.city_id, -- 城市id
	d.city_name,-- 城市名称
	d.city_seq, -- 带开城顺序城市名称
	yy.channel_type_id as media_id, -- 媒体类型ID
	case when (yy.channel_type = 1 or yy.channel_type = 3) and channel_type_name='神马' then 'UC'
	     else yy.channel_type_name end as media_name, -- 媒体类型名称
	yy.channel_type as module_id, -- 模块ID
	case 
		when yy.channel_type = 1 then "feed"
		when yy.channel_type = 2 then "导航"
		when yy.channel_type = 3 then "APP渠道"
		when yy.channel_type = 4 then "SEM"
		when yy.channel_type = 5 then "免费"
		when yy.channel_type = 6 and instr(yy.channel_name,'老客召回')>0 then "老客召回"
		when yy.channel_type = 7 then "端口"
		when yy.channel_type = 8 then "小程序"
		when yy.channel_type = 9  then "乐居"
		when yy.channel_type = 10 then "厂商"
		when yy.channel_type = 11 then "咨询师"
		when yy.channel_type = 12 then "厂商"
		when yy.channel_type = 13 then "开发商"
		when yy.channel_type = 14 then "开发商-B"
		when yy.channel_type = 15 then "商务投放"
		when yy.channel_type = 17 then "SEO"
	else "其他" end as module_name,-- 模块名称
	
	yy.device_type as device_id, -- 设备ID
	case 
		when yy.device_type = 1 then "PC"
		when yy.device_type = 2 then "M"
		when yy.device_type = 3 then "APP"
		when yy.device_type = 4 then "其他"
		when yy.device_type = 5 then "PCM"
		when yy.device_type = 6 then "微信小程序"
		when yy.device_type = 7 then "百度小程序"
		when yy.device_type = 8 then "微信公众号"    
		when yy.device_type = 9 then "支付宝小程序"   -- jbq add 20210518
		when yy.device_type = 10 then "高德小程序"    -- jbq add 20210518
		when yy.device_type = 11 then "应用市场"      -- jbq add 20210518
	else "其他" end as device_name, -- 设备名称
	
	-- 以下这2个属性，目前为null    
	-- jbq modifi 2021051 ,注释掉原有app_type_name
	-- t1.app_type as app_type_id,
	-- case 
	-- when t1.app_type = 1 then 'android'
	-- when t1.app_type = 2 then 'ios'
	-- else '未知' end as app_type_name,                        
	yy.app_id as app_type_id, -- APP类型ID         jiangbaoqiao modify 20210520                                          
	case when yy.app_id=301 then "微信小程序"
			 when yy.app_id=401 then "百度小程序"
			 when yy.app_id=101 then "安卓"
			 when yy.app_id=201 then "苹果"
			 when yy.app_id is null and instr(yy.channel_name,'安卓')>0 then "安卓" 
			 when yy.app_id is null and instr(yy.channel_name,'苹果')>0 then "苹果" 
			 else null end as app_type_name, -- APP类型名称        
	 
	coalesce(from_unixtime(yy.create_datetime),from_unixtime(yy.update_datetime)) as create_time,
	coalesce(from_unixtime(yy.update_datetime),from_unixtime(yy.create_datetime)) as update_time,
	yy.status, -- 状态，1有效，0无效
	yy.group_id,
	yy.utm_source,
	${hiveconf:etl_date} as pdate, -- 分区日期
	current_timestamp() as etl_time 
		
FROM
    (SELECT
            xx.channel_id,
            xx.channel_name,
            xx.status,
            xx.city_id,
            xx.group_id,
            -- 优先取：cj_agency里面的渠道
            coalesce(xx.channel_type_id_a,xx.channel_type_id) as channel_type_id,
            coalesce(xx.channel_type_name_a,xx.channel_type_name) as channel_type_name,
            coalesce(xx.channel_type_a,xx.channel_type) as channel_type,
            coalesce(xx.device_type_a,xx.device_type) as device_type,
            xx.create_datetime,
            xx.update_datetime,
            xx.app_id,
            xx.utm_source
    FROM
        (SELECT
                t.channel_id,
                t.channel_name,
                t.status,
                t.city_id,
                t.group_id,
                t.channel_type_id,
                t.channel_type_name,
                t.channel_type,    -- 渠道类型
                t.create_datetime,
                t.update_datetime,
                t.device_type,               
                p.channel_type_id as channel_type_id_a,
                p.channel_type_name as channel_type_name_a,
                p.channel_type as channel_type_a,
                p.device_type as device_type_a,
                p.app_id as app_id,
                p.utm_source as utm_source
        FROM ods.cj_channel t
        LEFT JOIN
            (SELECT
                    group_id,
                    channel_type_id,  -- 媒体分类id
                    channel_type_name,-- 媒体分类名称
                    if(app_id=301 or app_id=401,NULL,3) as channel_type,
                    if(app_id=301 or app_id=401,NULL,3) as device_type,
                    app_id,
                    utm_source -- jbq 用来标记广告来源
            FROM ods.cj_agency
            WHERE group_id is not null and group_id != 0
              and group_id !=32087 -- 重复数据
            ) p 
        ON t.group_id=p.group_id
        ) xx
    )yy
LEFT JOIN julive_dim.dim_city d
ON yy.city_id = d.city_id
;



DROP VIEW IF EXISTS db_l.deal_cj_channel;
CREATE VIEW IF NOT EXISTS db_l.deal_cj_channel (
channel_id                                                  COMMENT '渠道ID', 
channel_name                                                COMMENT '渠道名称', 
status                                                      COMMENT '状态',  
city_id                                                     COMMENT '渠道城市ID', 
city                                                        COMMENT '渠道城市名称',
group_id                                                    COMMENT '组ID',   
channel_type_id                                             COMMENT '媒体类型ID', 
media_type                                                  COMMENT '媒体名称',
product_type                                                COMMENT '产品类型',
create_time                                                 COMMENT '创建时间',
device_type                                                 COMMENT '设备类型',
app_type                                                    COMMENT 'app类型',
utm_source                                                  COMMENT '广告来源标记')
COMMENT '渠道维表'
AS SELECT
channel_id,
channel_name,
status,
city_id,
city_name,
group_id,
media_id,
media_name,
module_name,
create_time,
device_name,
app_type_name,
utm_source
FROM julive_dim.dim_channel_info
;