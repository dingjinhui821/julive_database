#!/bin/bash
#source /data1/etl/.bash_profile
source /etc/profile

DATE_ID=$(date -d "yesterday" +%Y%m%d )

echo $DATE_ID

hive -e "set mapreduce.job.name=srz_fact_searchword_report_day_cost;
INSERT OVERWRITE TABLE julive_fact.fact_searchword_report_day_cost partition(pdate='${DATE_ID}')
SELECT
    t1.creative_visit_url,
    t1.update_datetime,
    t1.click_rate,
    t1.creative_title,
    t1.creative_id,
    t1.creative_desc2,
    t1.creative_desc1,
    t1.media_type,
    t1.show_num,
    t1.create_datetime,
    t1.account_name,
    t1.dsp_account_id,
    t1.match_type,
    t1.search_word,
    t1.unit_id,
    t1.keyword_name,
    t1.default_visit_url,
    t1.report_date,
    t1.creator,
    round(t1.bill_cost/(1+t2.rebate),4) as cost,
    t1.is_add_negative,
    t1.creative_show_url,
    t1.click_num,
    t1.plan_name,
    t1.is_add_keyword,
    t1.average_ranking,
    t1.unit_name,
    t1.product_type,
    case when t1.media_type =2 then t3.keyword_name else t1.keyword_id end as keyword_id,
    --t1.keyword_id,
    t1.updator,
    t1.average_click_price,
    t1.plan_id,
    t1.mobile_visit_url,
    t1.device,
    t1.bill_cost
FROM (
    SELECT
        *
    FROM ods.searchword_report_day
    WHERE pdate = '${DATE_ID}'
)t1 LEFT JOIN (
    SELECT
        *
    FROM ods.dsp_account_rebate
)t2 ON t1.dsp_account_id = t2.dsp_account_id
LEFT JOIN julive_dim.dim_keyword_info t3 on t1.keyword_name=t3.keyword_name and t3.pdate = '${DATE_ID}'
WHERE unix_timestamp(t1.pdate,'yyyyMMdd') BETWEEN t2.start_date AND t2.end_date
;
"

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
    ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@18332118662,fact_searchword_report_day_cost FAILED\""    
    exit $EXCODE
fi


