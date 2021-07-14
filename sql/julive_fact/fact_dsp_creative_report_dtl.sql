--  功能描述：创意报告明细事实表，每天全量重跑
--  输 入 表：
--        ods.dsp_creative_report                                       -- 创意报告表
--
--
--
--  输 出 表：julive_fact.fact_dsp_creative_report_dtl                    -- 创意报告明细事实表，ods直抽到fact层，t+1跑全量数据
--
--
--  创 建 者：  蒋胜洋  13971279523  jiangshengyang@julive.com
--  创建日期： 2021/06/29 14:07
--
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期： 修改人   修改内容
--

set hive.execution.engine=spark;
set spark.app.name=fact_dsp_creative_report_dtl;
set mapred.job.name=fact_dsp_creative_report_dtl;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=4g;
set spark.executor.instances=24;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;

insert overwrite table julive_fact.fact_dsp_creative_report_dtl
select
    id, --id
    dsp_account_id, --市场投放账户id
    account_name, --市场投放账户名
    pic_num, --图片数量
    plan_name, --推广计划
    plan_id, --推广计划id
    unit_name, --推广单元
    unit_id, --推广单元id
    title, --创意标题
    creative_name, --创意名称
    creative_id, --创意id
    show_num, --展示次数
    click_num, --点击次数
    cost, --现金消费
    bill_cost, --账面消费
    click_rate, --点击率
    price, --出价
    average_click_price, --平均点击价格
    bid_type, --付费模式 0未识别 1点击 3转化
    device, --设备类型（0：全部；1：计算机；2：移动设备
    url, --url
    channel_id, --渠道id
    channel_put, --渠道投放
    clue_num, --线索量
    clue_cost, --线索成本
    distribute_amount, --上户量
    distribute_cost, --上户
    distribute_rate, --上户率
    from_unixtime(report_date, 'yyyy-MM-dd') as report_date, --日期
    from_unixtime(create_datetime, 'yyyy-MM-dd HH:mm:ss') as create_time, --创建时间
    from_unixtime(update_datetime, 'yyyy-MM-dd HH:mm:ss') as update_time, --更新时间
    creator, --创建人
    updator, --更新人
    status, --数据状态（1：从api同步完成；2：关键词匹配完成；3：关联分析完成）
    adgroup_id, --广告组id 微信广告主专用
    current_timestamp as etl_time -- ETL跑数时间
from ods.dsp_creative_report      -- 创意报告表
;




