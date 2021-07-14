-- 增量ETL 
-- dsp_account.media_type 取值 ：
-- case 
-- when t2.media_type = 1 then '百度' 
-- when t2.media_type = 2 then '360' 
-- when t2.media_type = 3 then '搜狗' 
-- when t2.media_type = 4 then '今日头条' 
-- when t2.media_type = 5 then '腾讯智汇推' 
-- when t2.media_type = 6 then '百度信息流' 
-- when t2.media_type = 7 then 'APP' 
-- when t2.media_type = 8 then '其他' 
-- when t2.media_type = 9 then '免费' 
-- when t2.media_type = 10 then '导航' 
-- when t2.media_type = 11 then '神马' 
-- when t2.media_type = 12 then '厂商' 
-- when t2.media_type = 13 then '微信' 
-- when t2.media_type = 14 then '端口' 
-- end 


set etl_date = '${hiveconf:etlDate}';
set etl_yestoday = '${hiveconf:etlYestoday}'; 
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

set hive.execution.engine=spark;
set spark.app.name=fact_dsp_sem_keyword_report_hour_dtl;
set mapred.job.name=fact_dsp_sem_keyword_report_hour_dtl;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;


with tmp_keyword_hour as (
select 

get_json_object(json,'$.channel_id') as channel_id,
get_json_object(json,'$.product_type') as product_type,
get_json_object(json,'$.media_type') as media_type,

get_json_object(json,'$.dsp_account_id') as dsp_account_id,
get_json_object(json,'$.account_name') as account_name,
get_json_object(json,'$.plan_id') as plan_id,
get_json_object(json,'$.plan_name') as plan_name,
get_json_object(json,'$.unit_id') as unit_id,
get_json_object(json,'$.unit_name') as unit_name,
get_json_object(json,'$.keyword_id') as keyword_id,
get_json_object(json,'$.keyword_name') as keyword_name,

get_json_object(json,'$.channel_put') as channel_put,
get_json_object(json,'$.device') as device,
get_json_object(json,'$.match_type') as match_type,

get_json_object(json,'$.show_num') as show_num,
get_json_object(json,'$.click_num') as click_num,
get_json_object(json,'$.cost') as cost,
get_json_object(json,'$.bill_cost') as bill_cost,
get_json_object(json,'$.average_ranking') as average_ranking,
get_json_object(json,'$.price') as price,
get_json_object(json,'$.url') as url,
get_json_object(json,'$.report_date') as report_date,
get_json_object(json,'$.hour') as report_hour

from ods.channel_report_day 
where pdate >= ${hiveconf:etl_yestoday}
and from_unixtime(cast(get_json_object(json,'$.report_date') as bigint),'yyyyMMdd') = ${hiveconf:etl_date}
and get_json_object(json,'$.pull_report_type')='hour_keyword_reprot'
)
,tmp_keyword as (
select 

t2.city_id as channel_city_id,
t2.city_name as channel_city_name,

case
when t1.url like '%julive.com/project/s/foshan%' then'20000191'
when t1.url like '%julive.com/gz/project/foshan%' then'20000191'
when t1.url like '%sh.comjia.com%' then'20000001'
when t1.url like '%tj.comjia.com%' then'20000019'
when t1.url like '%gz.comjia.com%' then'20000037'
when t1.url like '%su.comjia.com%' then'20000061'
when t1.url like '%hz.comjia.com%' then'20000074'
when t1.url like '%cd.comjia.com%' then'20000086'
when t1.url like '%cq.comjia.com%' then'20000050'
when t1.url like '%nj.comjia.com%' then'20000171'
when t1.url like '%zz.comjia.com%' then'20000158'
when t1.url like '%sz.comjia.com%' then'20000103'
when t1.url like '%sh.julive.com%' then'20000001'
when t1.url like '%tj.julive.com%' then'20000019'
when t1.url like '%gz.julive.com%' then'20000037'
when t1.url like '%su.julive.com%' then'20000061'
when t1.url like '%hz.julive.com%' then'20000074'
when t1.url like '%cd.julive.com%' then'20000086'
when t1.url like '%cq.julive.com%' then'20000050'
when t1.url like '%nj.julive.com%' then'20000171'
when t1.url like '%zz.julive.com%' then'20000158'
when t1.url like '%sz.julive.com%' then'20000103'
when t1.url like '%m.comjia.com/sh%' then'20000001'
when t1.url like '%m.comjia.com/tj%' then'20000019'
when t1.url like '%m.comjia.com/gz%' then'20000037'
when t1.url like '%m.comjia.com/su%' then'20000061'
when t1.url like '%m.comjia.com/cd%' then'20000086'
when t1.url like '%m.comjia.com/cq%' then'20000050'
when t1.url like '%m.comjia.com/nj%' then'20000171'
when t1.url like '%m.comjia.com/zz%' then'20000158'
when t1.url like '%m.comjia.com/sz%' then'20000103'
when t1.url like '%m.comjia.com/hz%' then'20000074'
when t1.url like '%m.julive.com/sh%' then'20000001'
when t1.url like '%m.julive.com/tj%' then'20000019'
when t1.url like '%m.julive.com/gz%' then'20000037'
when t1.url like '%m.julive.com/su%' then'20000061'
when t1.url like '%m.julive.com/cd%' then'20000086'
when t1.url like '%m.julive.com/cq%' then'20000050'
when t1.url like '%m.julive.com/nj%' then'20000171'
when t1.url like '%m.julive.com/zz%' then'20000158'
when t1.url like '%m.julive.com/sz%' then'20000103'
when t1.url like '%m.julive.com/hz%' then'20000074'

when t1.url like '%m.julive.com/fs%' then'20000191'
when t1.url like '%m.comjia.com/fs%' then'20000191'
when t1.url like '%fs.julive.com%' then'20000191'
when t1.url like '%fs.comjia.com%' then'20000191'

when t1.url like '%m.julive.com/cs%' then'20000199' -- 长沙 
when t1.url like '%m.comjia.com/cs%' then'20000199'
when t1.url like '%cs.julive.com%' then'20000199'
when t1.url like '%cs.comjia.com%' then'20000199'

when t1.url like '%m.julive.com/wh%' then'20000201' -- 武汉 
when t1.url like '%m.comjia.com/wh%' then'20000201'
when t1.url like '%wh.julive.com%' then'20000201'
when t1.url like '%wh.comjia.com%' then'20000201'

when t1.url like '%m.julive.com/xa%' then'20000200' -- 西安 
when t1.url like '%m.comjia.com/xa%' then'20000200'
when t1.url like '%xa.julive.com%' then'20000200'
when t1.url like '%xa.comjia.com%' then'20000200'

when t1.url like '%m.julive.com/nb%' then'20000172' -- 宁波
when t1.url like '%m.comjia.com/nb%' then'20000172'
when t1.url like '%nb.julive.com%' then'20000172'
when t1.url like '%nb.comjia.com%' then'20000172'

when t1.url like '%m.julive.com/jn%' then'20000247' -- 济南
when t1.url like '%m.comjia.com/jn%' then'20000247'
when t1.url like '%jn.julive.com%' then'20000247'
when t1.url like '%jn.comjia.com%' then'20000247'

when t1.url like '%m.julive.com/sy%' then'20000248' -- 沈阳
when t1.url like '%m.comjia.com/sy%' then'20000248'
when t1.url like '%sy.julive.com%' then'20000248'
when t1.url like '%sy.comjia.com%' then'20000248'

when t1.url like '%m.julive.com/qd%' then'20000249' -- 青岛
when t1.url like '%m.comjia.com/qd%' then'20000249'
when t1.url like '%qd.julive.com%' then'20000249'
when t1.url like '%qd.comjia.com%' then'20000249'

when t1.url like '%m.julive.com/dl%' then'20000250' -- 大连
when t1.url like '%m.comjia.com/dl%' then'20000250'
when t1.url like '%dl.julive.com%' then'20000250'
when t1.url like '%dl.comjia.com%' then'20000250'

when t1.url like '%m.julive.com/yt%' then'20000251' -- 烟台
when t1.url like '%m.comjia.com/yt%' then'20000251'
when t1.url like '%yt.julive.com%' then'20000251'
when t1.url like '%yt.comjia.com%' then'20000251'

when t1.url like '%m.julive.com/cc%' then'20000252' -- 长春
when t1.url like '%m.comjia.com/cc%' then'20000252'
when t1.url like '%cc.julive.com%' then'20000252'
when t1.url like '%cc.comjia.com%' then'20000252'

when t1.url like '%m.julive.com/hf%' then'20000253' -- 合肥
when t1.url like '%m.comjia.com/hf%' then'20000253'
when t1.url like '%hf.julive.com%' then'20000253'
when t1.url like '%hf.comjia.com%' then'20000253'

when t1.url like '%m.julive.com/sjz%' then'20000254' -- 石家庄
when t1.url like '%m.comjia.com/sjz%' then'20000254'
when t1.url like '%sjz.julive.com%' then'20000254'
when t1.url like '%sjz.comjia.com%' then'20000254'

when t1.url like '%m.julive.com/xm%' then'20000255' -- 厦门
when t1.url like '%m.comjia.com/xm%' then'20000255'
when t1.url like '%xm.julive.com%' then'20000255'
when t1.url like '%xm.comjia.com%' then'20000255'

when t1.url like '%m.julive.com/jx%' then'20000256' -- 嘉兴
when t1.url like '%m.comjia.com/jx%' then'20000256'
when t1.url like '%jx.julive.com%' then'20000256'
when t1.url like '%jx.comjia.com%' then'20000256'

when t1.url like '%m.julive.com/wx%' then'20000257' -- 无锡
when t1.url like '%m.comjia.com/wx%' then'20000257'
when t1.url like '%wx.julive.com%' then'20000257'
when t1.url like '%wx.comjia.com%' then'20000257'

when t1.url like '%m.julive.com/huzhou%' then'20000258' -- 湖州
when t1.url like '%m.comjia.com/huzhou%' then'20000258'
when t1.url like '%huzhou.julive.com%' then'20000258'
when t1.url like '%huzhou.comjia.com%' then'20000258'

when t1.url like '%m.julive.com/zj%' then'20000259' -- 镇江
when t1.url like '%m.comjia.com/zj%' then'20000259'
when t1.url like '%zj.julive.com%' then'20000259'
when t1.url like '%zj.comjia.com%' then'20000259'

when t1.url like '%m.julive.com/hui%' then'20000260' -- 惠州
when t1.url like '%m.comjia.com/hui%' then'20000260'
when t1.url like '%hui.julive.com%' then'20000260'
when t1.url like '%hui.comjia.com%' then'20000260'

when t1.url like '%m.julive.com/dg%' then'20000261' -- 东莞
when t1.url like '%m.comjia.com/dg%' then'20000261'
when t1.url like '%dg.julive.com%' then'20000261'
when t1.url like '%dg.comjia.com%' then'20000261'

when t1.url like '%m.julive.com/zs%' then'20000262' -- 中山
when t1.url like '%m.comjia.com/zs%' then'20000262'
when t1.url like '%zs.julive.com%' then'20000262'
when t1.url like '%zs.comjia.com%' then'20000262'

when t1.url like '%m.julive.com/my%' then'20000263' -- 绵阳
when t1.url like '%m.comjia.com/my%' then'20000263'
when t1.url like '%my.julive.com%' then'20000263'
when t1.url like '%my.comjia.com%' then'20000263'

when t1.url like '%m.julive.com/lf%' then'20000264' -- 廊坊
when t1.url like '%m.comjia.com/lf%' then'20000264'
when t1.url like '%lf.julive.com%' then'20000264'
when t1.url like '%lf.comjia.com%' then'20000264'

when t1.url like '%m.julive.com/xiangtan%' then'20000381' -- 湘潭
when t1.url like '%m.comjia.com/xiangtan%' then'20000381'
when t1.url like '%xiangtan.julive.com%' then'20000381'
when t1.url like '%xiangtan.comjia.com%' then'20000381'

when t1.url like '%m.julive.com/yy%' then'20000382' -- 岳阳
when t1.url like '%m.comjia.com/yy%' then'20000382'
when t1.url like '%yy.julive.com%' then'20000382'
when t1.url like '%yy.comjia.com%' then'20000382'

when t1.url like '%m.julive.com/jiaozhou%' then'20000473' -- 胶州
when t1.url like '%m.comjia.com/jiaozhou%' then'20000473'
when t1.url like '%jiaozhou.julive.com%' then'20000473'
when t1.url like '%jiaozhou.comjia.com%' then'20000473'

when t1.url like '%m.julive.com/ks%' then'20000265' -- 昆山
when t1.url like '%m.comjia.com/ks%' then'20000265'
when t1.url like '%ks.julive.com%' then'20000265'
when t1.url like '%ks.comjia.com%' then'20000265'

when t1.url like '%m.julive.com/changshu%' then'20000485' -- 常熟
when t1.url like '%m.comjia.com/changshu%' then'20000485'
when t1.url like '%changshu.julive.com%' then'20000485'
when t1.url like '%changshu.comjia.com%' then'20000485'

when t1.url like '%m.julive.com/sy%' then'20000248' -- 沈阳
when t1.url like '%m.comjia.com/sy%' then'20000248'
when t1.url like '%sy.julive.com%' then'20000248'
when t1.url like '%sy.comjia.com%' then'20000248'


when t1.url is null or t1.url='' then null
else '2' end as url_city_id, -- URL城市ID 

case
when t1.url like '%julive.com/project/s/foshan%' then'佛山'
when t1.url like '%julive.com/gz/project/foshan%' then'佛山'
when t1.url like '%sh.comjia.com%' then'上海'
when t1.url like '%tj.comjia.com%' then'天津'
when t1.url like '%gz.comjia.com%' then'广州'
when t1.url like '%su.comjia.com%' then'苏州'
when t1.url like '%hz.comjia.com%' then'杭州'
when t1.url like '%cd.comjia.com%' then'成都'
when t1.url like '%cq.comjia.com%' then'重庆'
when t1.url like '%nj.comjia.com%' then'南京'
when t1.url like '%zz.comjia.com%' then'郑州'
when t1.url like '%sz.comjia.com%' then'深圳'
when t1.url like '%sh.julive.com%' then'上海'
when t1.url like '%tj.julive.com%' then'天津'
when t1.url like '%gz.julive.com%' then'广州'
when t1.url like '%su.julive.com%' then'苏州'
when t1.url like '%hz.julive.com%' then'杭州'
when t1.url like '%cd.julive.com%' then'成都'
when t1.url like '%cq.julive.com%' then'重庆'
when t1.url like '%nj.julive.com%' then'南京'
when t1.url like '%zz.julive.com%' then'郑州'
when t1.url like '%sz.julive.com%' then'深圳'
when t1.url like '%m.comjia.com/sh%' then'上海'
when t1.url like '%m.comjia.com/tj%' then'天津'
when t1.url like '%m.comjia.com/gz%' then'广州'
when t1.url like '%m.comjia.com/su%' then'苏州'
when t1.url like '%m.comjia.com/cd%' then'成都'
when t1.url like '%m.comjia.com/cq%' then'重庆'
when t1.url like '%m.comjia.com/nj%' then'南京'
when t1.url like '%m.comjia.com/zz%' then'郑州'
when t1.url like '%m.comjia.com/sz%' then'深圳'
when t1.url like '%m.comjia.com/hz%' then'杭州'
when t1.url like '%m.julive.com/sh%' then'上海'
when t1.url like '%m.julive.com/tj%' then'天津'
when t1.url like '%m.julive.com/gz%' then'广州'
when t1.url like '%m.julive.com/su%' then'苏州'
when t1.url like '%m.julive.com/cd%' then'成都'
when t1.url like '%m.julive.com/cq%' then'重庆'
when t1.url like '%m.julive.com/nj%' then'南京'
when t1.url like '%m.julive.com/zz%' then'郑州'
when t1.url like '%m.julive.com/sz%' then'深圳'
when t1.url like '%m.julive.com/hz%' then'杭州'
when t1.url like '%m.julive.com/fs%' then'佛山'
when t1.url like '%m.comjia.com/fs%' then'佛山'
when t1.url like '%fs.julive.com%' then'佛山'
when t1.url like '%fs.comjia.com%' then'佛山'

when t1.url like '%m.julive.com/cs%' then'长沙' -- 长沙 
when t1.url like '%m.comjia.com/cs%' then'长沙'
when t1.url like '%cs.julive.com%' then'长沙'
when t1.url like '%cs.comjia.com%' then'长沙'

when t1.url like '%m.julive.com/wh%' then'武汉' -- 武汉 
when t1.url like '%m.comjia.com/wh%' then'武汉'
when t1.url like '%wh.julive.com%' then'武汉'
when t1.url like '%wh.comjia.com%' then'武汉'

when t1.url like '%m.julive.com/xa%' then'西安' -- 西安 
when t1.url like '%m.comjia.com/xa%' then'西安'
when t1.url like '%xa.julive.com%' then'西安'
when t1.url like '%xa.comjia.com%' then'西安'

when t1.url like '%m.julive.com/nb%' then'宁波' -- 宁波
when t1.url like '%m.comjia.com/nb%' then'宁波'
when t1.url like '%nb.julive.com%' then'宁波'
when t1.url like '%nb.comjia.com%' then'宁波'

when t1.url like '%m.julive.com/jn%' then'济南' -- 济南
when t1.url like '%m.comjia.com/jn%' then'济南'
when t1.url like '%jn.julive.com%' then'济南'
when t1.url like '%jn.comjia.com%' then'济南'

when t1.url like '%m.julive.com/sy%' then'沈阳' -- 沈阳
when t1.url like '%m.comjia.com/sy%' then'沈阳'
when t1.url like '%sy.julive.com%' then'沈阳'
when t1.url like '%sy.comjia.com%' then'沈阳'

when t1.url like '%m.julive.com/qd%' then'青岛' -- 青岛
when t1.url like '%m.comjia.com/qd%' then'青岛'
when t1.url like '%qd.julive.com%' then'青岛'
when t1.url like '%qd.comjia.com%' then'青岛'

when t1.url like '%m.julive.com/dl%' then'大连' -- 大连
when t1.url like '%m.comjia.com/dl%' then'大连'
when t1.url like '%dl.julive.com%' then'大连'
when t1.url like '%dl.comjia.com%' then'大连'

when t1.url like '%m.julive.com/yt%' then'烟台' -- 烟台
when t1.url like '%m.comjia.com/yt%' then'烟台'
when t1.url like '%yt.julive.com%' then'烟台'
when t1.url like '%yt.comjia.com%' then'烟台'

when t1.url like '%m.julive.com/cc%' then'长春' -- 长春
when t1.url like '%m.comjia.com/cc%' then'长春'
when t1.url like '%cc.julive.com%' then'长春'
when t1.url like '%cc.comjia.com%' then'长春'

when t1.url like '%m.julive.com/hf%' then'合肥' -- 合肥
when t1.url like '%m.comjia.com/hf%' then'合肥'
when t1.url like '%hf.julive.com%' then'合肥'
when t1.url like '%hf.comjia.com%' then'合肥'

when t1.url like '%m.julive.com/sjz%' then'石家庄' -- 石家庄
when t1.url like '%m.comjia.com/sjz%' then'石家庄'
when t1.url like '%sjz.julive.com%' then'石家庄'
when t1.url like '%sjz.comjia.com%' then'石家庄'

when t1.url like '%m.julive.com/xm%' then'厦门' -- 厦门
when t1.url like '%m.comjia.com/xm%' then'厦门'
when t1.url like '%xm.julive.com%' then'厦门'
when t1.url like '%xm.comjia.com%' then'厦门'

when t1.url like '%m.julive.com/jx%' then'嘉兴' -- 嘉兴
when t1.url like '%m.comjia.com/jx%' then'嘉兴'
when t1.url like '%jx.julive.com%' then'嘉兴'
when t1.url like '%jx.comjia.com%' then'嘉兴'

when t1.url like '%m.julive.com/wx%' then'无锡' -- 无锡
when t1.url like '%m.comjia.com/wx%' then'无锡'
when t1.url like '%wx.julive.com%' then'无锡'
when t1.url like '%wx.comjia.com%' then'无锡'

when t1.url like '%m.julive.com/huzhou%' then'湖州' -- 湖州
when t1.url like '%m.comjia.com/huzhou%' then'湖州'
when t1.url like '%huzhou.julive.com%' then'湖州'
when t1.url like '%huzhou.comjia.com%' then'湖州'

when t1.url like '%m.julive.com/zj%' then'镇江' -- 镇江
when t1.url like '%m.comjia.com/zj%' then'镇江'
when t1.url like '%zj.julive.com%' then'镇江'
when t1.url like '%zj.comjia.com%' then'镇江'

when t1.url like '%m.julive.com/hui%' then'惠州' -- 惠州
when t1.url like '%m.comjia.com/hui%' then'惠州'
when t1.url like '%hui.julive.com%' then'惠州'
when t1.url like '%hui.comjia.com%' then'惠州'

when t1.url like '%m.julive.com/dg%' then'东莞' -- 东莞
when t1.url like '%m.comjia.com/dg%' then'东莞'
when t1.url like '%dg.julive.com%' then'东莞'
when t1.url like '%dg.comjia.com%' then'东莞'

when t1.url like '%m.julive.com/zs%' then'中山' -- 中山
when t1.url like '%m.comjia.com/zs%' then'中山'
when t1.url like '%zs.julive.com%' then'中山'
when t1.url like '%zs.comjia.com%' then'中山'

when t1.url like '%m.julive.com/my%' then'绵阳' -- 绵阳
when t1.url like '%m.comjia.com/my%' then'绵阳'
when t1.url like '%my.julive.com%' then'绵阳'
when t1.url like '%my.comjia.com%' then'绵阳'

when t1.url like '%m.julive.com/lf%' then'廊坊' -- 廊坊
when t1.url like '%m.comjia.com/lf%' then'廊坊'
when t1.url like '%lf.julive.com%' then'廊坊'
when t1.url like '%lf.comjia.com%' then'廊坊'

when t1.url like '%m.julive.com/xiangtan%' then'湘潭' -- 湘潭
when t1.url like '%m.comjia.com/xiangtan%' then'湘潭'
when t1.url like '%xiangtan.julive.com%' then'湘潭'
when t1.url like '%xiangtan.comjia.com%' then'湘潭'

when t1.url like '%m.julive.com/yy%' then'岳阳' -- 岳阳
when t1.url like '%m.comjia.com/yy%' then'岳阳'
when t1.url like '%yy.julive.com%' then'岳阳'
when t1.url like '%yy.comjia.com%' then'岳阳'

when t1.url like '%m.julive.com/jiaozhou%' then'胶州' -- 胶州
when t1.url like '%m.comjia.com/jiaozhou%' then'胶州'
when t1.url like '%jiaozhou.julive.com%' then'胶州'
when t1.url like '%jiaozhou.comjia.com%' then'胶州'

when t1.url like '%m.julive.com/ks%' then'昆山' -- 昆山
when t1.url like '%m.comjia.com/ks%' then'昆山'
when t1.url like '%ks.julive.com%' then'昆山'
when t1.url like '%ks.comjia.com%' then'昆山'

when t1.url like '%m.julive.com/changshu%' then'常熟' -- 常熟
when t1.url like '%m.comjia.com/changshu%' then'常熟'
when t1.url like '%changshu.julive.com%' then'常熟'
when t1.url like '%changshu.comjia.com%' then'常熟'

when t1.url like '%m.julive.com/sy%' then'沈阳' -- 沈阳
when t1.url like '%m.comjia.com/sy%' then'沈阳'
when t1.url like '%sy.julive.com%' then'沈阳'
when t1.url like '%sy.comjia.com%' then'沈阳'

when t1.url is null or t1.url = '' then '没有url'
else '北京' end as url_city_name, -- URL城市名称 

case
when t1.channel_put like '%天津%' then'20000019'
when t1.channel_put like '%北京%' then'2'
when t1.channel_put like '%上海%' then'20000001'
when t1.channel_put like '%苏州%' then'20000061'
when t1.channel_put like '%杭州%' then'20000074'
when t1.channel_put like '%南京%' then'20000171'
when t1.channel_put like '%广州%' then'20000037'
when t1.channel_put like '%深圳%' then'20000103'
when t1.channel_put like '%重庆%' then'20000050'
when t1.channel_put like '%成都%' then'20000086'
when t1.channel_put like '%郑州%' then'20000158'
when t1.channel_put like '%佛山%' then'20000191'

when t1.channel_put like '%长沙%' then'20000199'
when t1.channel_put like '%武汉%' then'20000201'
when t1.channel_put like '%西安%' then'20000200'

when t1.channel_put like '%宁波%' then'20000172'
when t1.channel_put like '%济南%' then'20000247'
when t1.channel_put like '%沈阳%' then'20000248'
when t1.channel_put like '%青岛%' then'20000249'
when t1.channel_put like '%大连%' then'20000250'
when t1.channel_put like '%烟台%' then'20000251'
when t1.channel_put like '%长春%' then'20000252'
when t1.channel_put like '%合肥%' then'20000253'
when t1.channel_put like '%石家庄%' then'20000254'
when t1.channel_put like '%厦门%' then'20000255'
when t1.channel_put like '%嘉兴%' then'20000256'
when t1.channel_put like '%无锡%' then'20000257'
when t1.channel_put like '%湖州%' then'20000258'
when t1.channel_put like '%镇江%' then'20000259'
when t1.channel_put like '%惠州%' then'20000260'
when t1.channel_put like '%东莞%' then'20000261'
when t1.channel_put like '%中山%' then'20000262'
when t1.channel_put like '%绵阳%' then'20000263'
when t1.channel_put like '%廊坊%' then'20000264'
when t1.channel_put like '%湘潭%' then'20000381'
when t1.channel_put like '%岳阳%' then'20000382'
when t1.channel_put like '%胶州%' then'20000473'

when t1.channel_put like '%昆山%' then'20000265'
when t1.channel_put like '%常熟%' then'20000485'
when t1.channel_put like '%沈阳%' then'20000248'

else null end as kw_city_id, -- 关键词城市ID

case
when t1.channel_put like '%天津%' then'天津'
when t1.channel_put like '%北京%' then'北京'
when t1.channel_put like '%上海%' then'上海'
when t1.channel_put like '%苏州%' then'苏州'
when t1.channel_put like '%杭州%' then'杭州'
when t1.channel_put like '%南京%' then'南京'
when t1.channel_put like '%广州%' then'广州'
when t1.channel_put like '%深圳%' then'深圳'
when t1.channel_put like '%重庆%' then'重庆'
when t1.channel_put like '%成都%' then'成都'
when t1.channel_put like '%郑州%' then'郑州'
when t1.channel_put like '%佛山%' then'佛山'

when t1.channel_put like '%长沙%' then'长沙'
when t1.channel_put like '%武汉%' then'武汉'
when t1.channel_put like '%西安%' then'西安'

when t1.channel_put like '%宁波%' then'宁波'
when t1.channel_put like '%济南%' then'济南'
when t1.channel_put like '%沈阳%' then'沈阳'
when t1.channel_put like '%青岛%' then'青岛'
when t1.channel_put like '%大连%' then'大连'
when t1.channel_put like '%烟台%' then'烟台'
when t1.channel_put like '%长春%' then'长春'
when t1.channel_put like '%合肥%' then'合肥'
when t1.channel_put like '%石家庄%' then'石家庄'
when t1.channel_put like '%厦门%' then'厦门'
when t1.channel_put like '%嘉兴%' then'嘉兴'
when t1.channel_put like '%无锡%' then'无锡'
when t1.channel_put like '%湖州%' then'湖州'
when t1.channel_put like '%镇江%' then'镇江'
when t1.channel_put like '%惠州%' then'惠州'
when t1.channel_put like '%东莞%' then'东莞'
when t1.channel_put like '%中山%' then'中山'
when t1.channel_put like '%绵阳%' then'绵阳'
when t1.channel_put like '%廊坊%' then'廊坊'
when t1.channel_put like '%湘潭%' then'湘潭'
when t1.channel_put like '%岳阳%' then'岳阳'
when t1.channel_put like '%胶州%' then'胶州'

when t1.channel_put like '%昆山%' then'昆山'
when t1.channel_put like '%常熟%' then'常熟'
when t1.channel_put like '%沈阳%' then'沈阳'

else null end as kw_city_name, -- 关键词城市名称 

t1.channel_id,
t2.channel_name,

t1.media_type as media_id,
case 
when t1.media_type = 1 then '百度' 
when t1.media_type = 2 then '360' 
when t1.media_type = 3 then '搜狗' 
when t1.media_type = 4 then '今日头条' 
when t1.media_type = 5 then '腾讯智汇推' 
when t1.media_type = 6 then '百度信息流' 
when t1.media_type = 7 then 'APP' 
when t1.media_type = 8 then '其他' 
when t1.media_type = 9 then '免费' 
when t1.media_type = 10 then '导航' 
when t1.media_type = 11 then '神马' 
when t1.media_type = 12 then '厂商' 
when t1.media_type = 13 then '微信' 
when t1.media_type = 14 then '端口' 
end as media_name,

t1.product_type as module_id,
case 
when t1.product_type = 1 then 'FEED'
when t1.product_type = 3 then 'APP'
when t1.product_type = 4 then 'SEM'
when t1.product_type = 0 then '全部'
end as module_name,

t1.device as device_id,
case 
when t1.device = 1 then 'PC'
when t1.device = 2 then 'M'
end as device_name,

t1.dsp_account_id as account_id,
t1.account_name,
t1.plan_id,
t1.plan_name,
t1.unit_id,
t1.unit_name,
t1.keyword_id,
t1.keyword_name,
t1.match_type,
t1.channel_put,
from_unixtime(cast(t1.report_date as bigint),'yyyy-MM-dd') as report_date_fat,
t1.report_date,
if(t1.report_hour < 10,concat('0',t1.report_hour),t1.report_hour) as report_hour,

coalesce(t1.show_num,0) as show_num,
coalesce(t1.click_num,0) as click_num,
coalesce(t1.bill_cost,0) as bill_cost,
coalesce(t1.cost,0) as cost,

t1.average_ranking,
t1.price,

from_unixtime(cast(t1.report_date as bigint),'yyyyMMdd') as pdate,

case 
when t1.media_type = 1 then 'baidu' 
when t1.media_type = 2 then '360' 
when t1.media_type = 3 then 'sougou' 
when t1.media_type = 4 then 'toutiao' 
when t1.media_type = 5 then 'tengxun' 
when t1.media_type = 6 then 'baiduxinxi' 
when t1.media_type = 11 then 'shenma' 
when t1.media_type = 13 then 'weixin' 
else 'others' end psrc 

from tmp_keyword_hour t1 
left join julive_dim.dim_channel_info t2 on t1.channel_id = t2.channel_id 

),
tmp_rebate as (
select * 
from ods.dsp_account_rebate 
where status = 1 
)

-- 计算返点后的cost值 
insert overwrite table julive_fact.fact_dsp_sem_keyword_report_hour_dtl partition(pdate,psrc)

select 

t1.channel_city_id,
t1.channel_city_name,
t1.url_city_id,
t1.url_city_name,
t1.kw_city_id,
t1.kw_city_name,
t1.channel_id,
t1.channel_name,
t1.media_id,
t1.media_name,
t1.module_id,
t1.module_name,
t1.device_id,
t1.device_name,
t1.account_id,
t1.account_name,
t1.plan_id,
t1.plan_name,
t1.unit_id,
t1.unit_name,
t1.keyword_id,
t1.keyword_name,
t1.match_type,
t1.channel_put,
t1.report_date_fat as report_date,
t1.report_hour as data_hour,

t1.show_num,
t1.click_num,
t1.bill_cost,
round(t1.bill_cost / (1 + t2.rebate),4) as cost,
t1.average_ranking,
t1.price,
current_timestamp() as etl_time,
t1.pdate,
t1.psrc 

from tmp_keyword t1 
left join tmp_rebate t2 on t1.account_id = t2.dsp_account_id 
where t1.report_date between t2.start_date and t2.end_date or t2.id is null 
;


-- 计算keyword汇总指标表 
insert overwrite table julive_fact.fact_dsp_sem_keyword_hour_indi partition(pdate,psrc) 

select 

channel_city_id,
channel_city_name,
channel_id,
channel_name,
media_id,
media_name,
module_id,
module_name,
account_id,
account_name,
plan_id,
plan_name,
unit_id,
unit_name,
keyword_id,
keyword_name,
channel_put,

concat(regexp_replace(report_date,"-",""),data_hour) as report_hour,

sum(t1.show_num) as show_num,
sum(t1.click_num) as click_num,
sum(t1.cost) as cost,
current_timestamp() as etl_time,
t1.pdate,
t1.psrc 

from julive_fact.fact_dsp_sem_keyword_report_hour_dtl t1 
where t1.pdate = ${hiveconf:etl_date}
and t1.psrc != 'others'

group by 
channel_city_id,
channel_city_name,
channel_id,
channel_name,
media_id,
media_name,
module_id,
module_name,
account_id,
account_name,
plan_id,
plan_name,
unit_id,
unit_name,
keyword_id,
keyword_name,
channel_put,
report_date,
data_hour,
t1.pdate,
t1.psrc 
;

