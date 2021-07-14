-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- scheam  
-- drop table if exists julive_fact.fact_hotline_call_log_dtl;
-- create table julive_fact.fact_hotline_call_log_dtl(
-- 
-- record_type                   int          comment '记录类型：1-呼入 2-呼出', 
-- called_id                     bigint       comment '业务主键',
-- customer_number               string       comment '客户号码',
-- customer_area_code            string       comment '呼入/外呼座席接听后的座席区号',
-- customer_number_type          int          comment '来电/外呼客户号码类型: 1-固话 2-手机 0-未知',
-- customer_number_type_tc       string       comment '来电/外呼客户号码类型: 1-固话 2-手机 0-未知',
-- call_type                     int          comment '呼叫类型: 1-呼入 2-WEB400呼入 3-点击外呼 4-预览外呼 5-预测外呼', 
-- call_type_tc                  string       comment '呼叫类型: 1-呼入 2-WEB400呼入 3-点击外呼 4-预览外呼 5-预测外呼', 
-- status                        int          comment '通话状态: 0-未知 1-来电/外呼响铃 2-已接通 3-未接通 4-挂机',
-- status_tc                     string       comment '通话状态: 0-未知 1-来电/外呼响铃 2-已接通 3-未接通 4-挂机',
-- cdr_start_time                string       comment '进入系统时间:yyyy-MM-dd HH:mm:ss',
-- cdr_answer_time               string       comment '系统接听时间:yyyy-MM-dd HH:mm:ss',
-- cdr_end_time                  string       comment '挂机时间:yyyy-MM-dd HH:mm:ss',
-- cdr_status                    int          comment '通话状态',
-- cdr_status_tc                 string       comment '通话状态中文注释',
-- wait_secs                     int          comment '等待时长(秒)',
-- conn_secs                     int          comment '通话时长(秒)',
-- all_secs                      int          comment '总时长(秒)',
-- call_result                   string       comment '呼叫结果', 
-- end_type                      int          comment '结束类型：1-其它（除11,12,13之前的）11-用户挂断 12-坐席挂断 13-转接', 
-- end_type_tc                   string       comment '结束类型：1-其它（除11,12,13之前的）11-用户挂断 12-坐席挂断 13-转接', 
-- satisfy_code                  int          comment '通话评价：1-满意 2-服务态度差 3-造成打扰 4-未解答问题 -1-用户挂机 -2-超时未评价 -3-未评价挂机 -4-坐席挂机 0-初始默认', 
-- satisfy_code_tc               string       comment '通话评价：1-满意 2-服务态度差 3-造成打扰 4-未解答问题 -1-用户挂机 -2-超时未评价 -3-未评价挂机 -4-坐席挂机 0-初始默认', 
-- cno                           string       comment '坐席工号',
-- cname                         string       comment '坐席姓名', 
-- city_id                       bigint       comment '城市ID', 
-- city_name                     string       comment '城市名称',
-- 
-- -- 冗余线索属性 
-- clue_id                       bigint       comment '线索ID', 
-- distribute_time               string       comment '线索分配时间:yyyy-MM-dd HH:mm:ss', 
-- project_type                  string       comment '业态', 
-- project_type_name             string       comment '业态类型', 
-- interest_project              string       comment '意向楼盘列表', 
-- intent_low_time               string       comment '无意向时间', 
-- clue_city_id                  int          comment '线索城市ID', 
-- clue_city_name                string       comment '线索城市名称', 
-- clue_city_seq                 string       comment '带开城顺序的线索城市名称', 
-- customer_intent_city_id       int          comment '客户意向城市ID', 
-- customer_intent_city_name     string       comment '客户意向城市名称', 
-- customer_intent_city_seq      string       comment '带开城顺序的客户意向城市名称', 
-- server_distribute_time        string       comment '客服分配时间:yyyy-MM-dd HH:mm:ss',
-- re_distribute_time            string       comment '重新分配时间:yyyy-MM-dd HH:mm:ss',
-- clue_create_time              string       comment '线索创建时间:yyyy-MM-dd HH:mm:ss',
-- -- 创建人维度 
-- creator                       string       comment '创建人ID',
-- creator_name                  string       comment '创建人姓名',
-- creator_post_id               int          comment '创建人岗位id',
-- creator_post_name             string       comment '创建人职位',
-- creator_dept_id               bigint       comment '创建人部门ID', 
-- creator_dept_name             string       comment '创建人部门名称', 
-- -- 跟进人维度 
-- follow_service                int          comment '跟进客服ID',
-- follow_service_name           string       comment '跟进客服名称',
-- follow_post_id                int          comment '跟进客服岗位id',
-- follow_post_name              string       comment '跟进客服职位',
-- follow_dept_id                bigint       comment '跟进客服部门ID', 
-- follow_dept_name              string       comment '跟进客服部门名称', 
-- is_distribute                 int          comment '是否分配,及不分配的原因', 
-- distribute_tc                 string       comment '是否分配,及不分配的原因',
-- clue_status                   string       comment '线索状态ID', 
-- clue_status_tc                string       comment '线索状态Name', 
-- 
-- channel_id                    int          comment '渠道ID', 
-- channel_name                  string       comment '渠道名称', 
-- media_id                      int          comment '媒体ID', 
-- media_name                    string       comment '媒体名称', 
-- module_id                     int          comment '模块ID', 
-- module_name                   string       comment '模块名称', 
-- device_id                     int          comment '设备ID', 
-- device_name                   string       comment '设备名称', 
-- 
-- -- 冗余员工属性 
-- job_number                    string       comment '员工工号', 
-- emp_id                        bigint       comment '咨询师id', 
-- emp_name                      string       comment '咨询师姓名', 
-- direct_leader_id              bigint       comment '组长id', 
-- direct_leader_name            string       comment '组长姓名', 
-- indirect_leader_id            int          comment '部门经理ID', 
-- indirect_leader_name          string       comment '部门经理名称', 
-- dept_id                       bigint       comment '部门ID', 
-- dept_name                     string       comment '部门名称', 
-- entry_date                    string       comment '入职日期', 
-- full_date                     string       comment '转正日期', 
-- offjob_date                   string       comment '离职时间', 
-- full_type                     int          comment '转正状态 1未转正 2已转正', 
-- full_type_tc                  string       comment '转正状态 1未转正 2已转正', 
-- post_id                       bigint       comment '员工岗位ID', -- 20201230 添加 
-- post_name                     string       comment '员工岗位名称',
-- 
-- create_time                   string       comment '创建时间:yyyy-MM-dd HH:mm:ss',
-- update_time                   string       comment '更新时间:yyyy-MM-dd HH:mm:ss',
-- 
-- etl_time                      string       comment 'ETL跑数时间:yyyy-MM-dd HH:mm:ss' 
-- 
-- ) comment '居理客服通话事实表' 
-- stored as parquet 
-- ;
-- 

-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- script 
-- #!/bin/bash
-- source /etc/profile
-- source ~/.bash_profile
-- 
-- if [[ $# == 1 ]];then
--     dateID=$1
-- elif [[ $# == 0 ]];then
--     dateID=$(date -d "-1 day" +"%Y%m%d")
-- else
--     echo "Usage sh "$0" dateID or sh "$0
--     exit 1
-- fi
-- 
-- 
-- #1 计算日期 
-- etlTomorrow=$(date -d "${dateID} 1 days" +"%Y%m%d")
-- etlYestoday=$(date -d "${dateID} 1 days ago" +"%Y%m%d")
-- 
-- echo 日期参数: ${dateID} ${etlTomorrow} ${etlYestoday} 
-- 
-- 
-- #2、配置任务信息：sql文件路径和日志文件路径（日志文件需要通过日期时间配置唯一文件名）
-- sqlFile="/data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_hotline_call_log_dtl.sql"
-- logFile="/data4/nfsdata/julive_dw/etl/logs/julive_fact/fact_hotline_call_log_dtl${pre}_${dateID}_${RANDOM}.log"
-- 
-- echo "输出日期文件路径: "${logFile}
-- echo "开始执行脚本: hive -hiveconf etlDate=${dateID} -hiveconf etlYestoday=${etlYestoday} -hiveconf etlTomorrow=${etlTomorrow} -f ${sqlFile}"
-- hive -hiveconf etlDate=${dateID} -hiveconf etlYestoday=${etlYestoday} -hiveconf etlTomorrow=${etlTomorrow} -f ${sqlFile} 2> ${logFile} 
-- 
-- EXCODE=$?
-- if [ $EXCODE -eq 0 ]; then
--     echo "********************************SUCCEED*********************************"
-- else
--     echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
-- ssh -p10001 test01 <<eeooff
-- python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15998885390: etl表(fact_hotline_call_log_dtl)ETL发生异常，请尽快处理!"
-- eeooff
--     exit $EXCODE
-- fi
-- 


-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ETL  

set spark.app.name=fact_hotline_call_log_dtl;
set mapred.job.name=fact_hotline_call_log_dtl;
set spark.yarn.queue=etl;
set mapreduce.job.queuename=root.etl;
insert overwrite table julive_fact.fact_hotline_call_log_dtl 

select 

t1.record_type                                                               as record_type, -- 记录类型：1-呼入 2-呼出
t1.id                                                                        as called_id, -- 业务主键 
t1.customer_number                                                           as customer_number, -- 客户号码 
t1.customer_area_code                                                        as customer_area_code, -- 呼入/外呼座席接听后的座席区号 

t1.customer_number_type                                                      as customer_number_type, -- 来电或外呼客户号码类型: 2手机 1固话 0未知
case 
when t1.customer_number_type = 1 then '固话' 
when t1.customer_number_type = 2 then '手机' 
else '未知' end                                                              as customer_number_type_tc, -- customer_number_type转码 

t1.call_type                                                                 as call_type, -- 呼叫类型: 1-呼入 2-WEB400呼入 3-点击外呼 4-预览外呼 5-预测外呼 
case 
when t1.call_type = 1 then '呼入'
when t1.call_type = 2 then 'WEB400呼入'
when t1.call_type = 3 then '点击外呼'
when t1.call_type = 4 then '预览外呼'
when t1.call_type = 5 then '预测外呼' 
else '未知' end                                                              as call_type_tc, -- call_type转码 

t1.status                                                                    as status, -- 通话状态 0-未知 1-来电/外呼响铃 2-已接通 3-未接通 4-挂机
case 
when t1.status = 1 then '来电/外呼响铃'
when t1.status = 2 then '已接通'
when t1.status = 3 then '未接通'
when t1.status = 4 then '挂机'
else '未知' end                                                              as status_tc, -- status 转码 

from_unixtime(t1.cdr_start_time)                                             as cdr_start_time,
from_unixtime(t1.cdr_answer_time)                                            as cdr_answer_time,
from_unixtime(t1.cdr_end_time)                                               as cdr_end_time,

t1.cdr_status                                                                as cdr_status, -- 通话状态 
case 
when t1.cdr_status = 1 then '座席接听' 
when t1.cdr_status = 2 then '已呼叫座席座席未接听' 
when t1.cdr_status = 3 then '系统接听' 
when t1.cdr_status = 4 then '系统未接-ivr配置错误' 
when t1.cdr_status = 5 then '系统未接-停机' 
when t1.cdr_status = 6 then '系统未接-欠费' 
when t1.cdr_status = 7 then '系统未接-黑名单' 
when t1.cdr_status = 8 then '系统未接-未注册' 
when t1.cdr_status = 9 then '系统未接-彩铃' 
when t1.cdr_status = 10 then '网上400未接受' 
when t1.cdr_status = 11 then '系统未接-呼叫超出营帐中设置的最大限制' 
when t1.cdr_status = 12 then '其他错误' 

when t1.cdr_status = 21 then '(点击外呼、预览外呼时)座席接听，客户未接听(超时)' -- out 
when t1.cdr_status = 22 then '(点击外呼、预览外呼时)座席接听，客户未接听(空号拥塞)' 
when t1.cdr_status = 24 then '(点击外呼、预览外呼时)座席未接听' 
when t1.cdr_status = 28 then '双方接听' 
else '未知错误' end                                                          as cdr_status_tc, -- cdr_status 转码 

t1.wait_secs                                                                 as wait_secs,
t1.conn_secs                                                                 as conn_secs,
t1.all_secs                                                                  as all_secs,
t1.result                                                                    as call_result,

t1.end_type                                                                  as end_type, -- 1: 其它（除11,12,13之前的）11用户挂断,12坐席挂断 13转接 
case 
when t1.end_type = 1 then '其它(除11,12,13之前的)' 
when t1.end_type = 11 then '用户挂断' 
when t1.end_type = 12 then '坐席挂断' 
when t1.end_type = 13 then '转接' 
else '未知' end                                                              as end_type_tc, -- end_type 转码 

t1.satisfy_code                                                              as satisfy_code, -- 1=满意2=服务态度差3=造成打扰4=未解答问题-1:用户挂机-2:超时未评价-3:未评价挂机-4:坐席挂机0:初始默认
case 
when t1.satisfy_code = 1 then '满意' 
when t1.satisfy_code = 2 then '服务态度差' 
when t1.satisfy_code = 3 then '造成打扰' 
when t1.satisfy_code = 4 then '未解答问题' 
when t1.satisfy_code = -1 then '用户挂机' 
when t1.satisfy_code = -2 then '超时未评价' 
when t1.satisfy_code = -3 then '未评价挂机' 
when t1.satisfy_code = -4 then '坐席挂机' 
when t1.satisfy_code = 0 then '初始默认' 
else '未知' end                                                              as satisfy_code_tc, -- satisfy_code 转码 

t1.cno                                                                       as cno, -- 坐席工号
t1.cname                                                                     as cname, 
t1.city_id                                                                   as city_id,
t2.city_name                                                                 as city_name,

-- 线索数据 
t1.order_id                                                                  as clue_id,
t3.distribute_time                                                           as distribute_time,
t3.project_type                                                              as project_type,
t3.project_type_name                                                         as project_type_name,
t3.interest_project                                                          as interest_project,
t3.intent_low_time                                                           as intent_low_time,
t3.city_id                                                                   as clue_city_id,
t3.city_name                                                                 as clue_city_name,
t3.city_seq                                                                  as clue_city_seq,
t3.customer_intent_city_id                                                   as customer_intent_city_id,
t3.customer_intent_city_name                                                 as customer_intent_city_name,
t3.customer_intent_city_seq                                                  as customer_intent_city_seq,
t3.server_distribute_time                                                    as server_distribute_time,
t3.re_distribute_time                                                        as re_distribute_time,
t3.create_time                                                               as clue_create_time,
t3.creator                                                                   as creator,
t7.emp_name                                                                  as creator_name,
t7.post_id                                                                   as creator_post_id,
t7.post_name                                                                 as creator_post_name,
t7.dept_id                                                                   as creator_dept_id,
t7.dept_name                                                                 as creator_dept_name,
t3.follow_service                                                            as follow_service,
t6.emp_name                                                                  as follow_service_name,
t6.post_id                                                                   as follow_post_id,
t6.post_name                                                                 as follow_post_name,
t6.dept_id                                                                   as follow_dept_id,
t6.dept_name                                                                 as follow_dept_name,
t3.is_distribute                                                             as is_distribute,
t3.distribute_tc                                                             as distribute_tc,
t3.clue_status                                                               as clue_status,
t3.clue_status_tc                                                            as clue_status_tc,

t3.channel_id                                                                as channel_id,
t3.channel_name                                                              as channel_name,
t4.media_id                                                                  as media_id,
t4.media_name                                                                as media_name,
t4.module_id                                                                 as module_id,
t4.module_name                                                               as module_name,
t4.device_id                                                                 as device_id,
t4.device_name                                                               as device_name,

-- 员工数据 
t1.job_number                                                                as job_number,
t5.emp_id                                                                    as emp_id,
t5.emp_name                                                                  as emp_name,
t5.direct_leader_id                                                          as direct_leader_id,
t5.direct_leader_name                                                        as direct_leader_name,
t5.indirect_leader_id                                                        as indirect_leader_id,
t5.indirect_leader_name                                                      as indirect_leader_name,
t5.dept_id                                                                   as dept_id,
t5.dept_name                                                                 as dept_name,
t5.entry_date                                                                as entry_date,
t5.full_date                                                                 as full_date,
t5.offjob_date                                                               as offjob_date,
t5.full_type                                                                 as full_type,
t5.full_type_tc                                                              as full_type_tc,
t5.post_id                                                                   as post_id,
t5.post_name                                                                 as post_name,

from_unixtime(t1.create_datetime)                                            as create_time,
from_unixtime(t1.update_datetime)                                            as update_time,

current_timestamp()                                                          as etl_time 

from (

select 

1 as record_type,
t.id,
t.customer_number,
t.customer_area_code,
t.customer_number_type,
t.call_type,
t.status,
t.cdr_start_time,
t.cdr_answer_time,
t.cdr_end_time,
t.cdr_status,
t.wait_secs,
t.conn_secs,
t.all_secs,
t.result,
t.end_type,
t.satisfy_code,
t.cno,
t.cname,
t.city_id,
t.order_id,
t.job_number,
t.create_datetime,
t.update_datetime 

from ods.yw_hotline_record_in t 

union all 

select 

2 as record_type,
t.id,
t.customer_number,
t.customer_area_code,
t.customer_number_type,
t.call_type,
t.status,
t.cdr_start_time,
t.cdr_answer_time,
t.cdr_end_time,
t.cdr_status,
t.wait_secs,
t.conn_secs,
t.all_secs,
t.result,
t.end_type,
t.satisfy_code,
t.cno,
t.cname,
t.city_id,
t.order_id,
t.job_number,
t.create_datetime,
t.update_datetime 

from ods.yw_hotline_record_out t 

) t1 
left join julive_dim.dim_city t2 on t1.city_id = t2.city_id 
left join julive_dim.dim_clue_base_info t3 on t1.order_id = t3.clue_id 
left join julive_dim.dim_channel_info t4 on t3.channel_id = t4.channel_id 

left join julive_dim.dim_employee_info t5 on t1.job_number = t5.job_number 
 and t5.pdate = regexp_replace(to_date(from_unixtime(if(t1.cdr_start_time is not null or t1.cdr_start_time > 0,t1.cdr_start_time,t1.create_datetime))),'-','') 
 
left join julive_dim.dim_employee_info t6 on t3.follow_service = t6.emp_id 
 and t6.pdate = regexp_replace(to_date(if(t3.distribute_time is not null or t3.distribute_time != '',t3.distribute_time,t3.create_time)),'-','') 
 
left join julive_dim.dim_employee_info t7 on t3.creator = t7.emp_id 
 and t7.pdate = regexp_replace(t3.create_date,'-','') 
;

