--  功能描述：咨询师通话明细，每天全量重跑
--  输 入 表：
--        ods.yw_sys_number_talking                                     -- 通话相关
--        julive_dim.dim_clue_info                                      -- 线索维度基表-自营(居理数据)
--        julive_dim.dim_channel_info                                   -- 渠道维度表
--        julive_dim.dim_ps_employee_info                               -- 自营员工维度快照表
--        ods.yw_department_architecture_history                        -- 部门信息历史表
--        julive_dim.dim_city                                           -- 城市纬度表
--
--
--  输 出 表：julive_fact.fact_consultant_call_dtl                        -- 咨询师通话明细，t+1跑全量数据
--
--
--  创 建 者：  蒋胜洋  13971279523  jiangshengyang@julive.com
--  创建日期： 2021/06/16 18:51
--
--  整合：julive_fact.fact_consultant_call_log_dtl 和 dwd.consultant_called_log_report 表，分析如下
--  fact_clue_full_line_indi.sql
--  fact_consultant_call_log_dtl.sql
--  julive_fact.fact_consultant_call_log_dtl 是个view，yw_sys_number_talking为主表，取近90天的数据，order_id不能为空。按t+1跑数据
--
--  yw_order1.hql
--  app_market_full_process_yw.sql
--  consultant_called_log_report.sql脚本中，dwd.consultant_called_log_report---(轻微聚合)--->dwd.consultant_called_log_clue_report
--  dwd.consultant_called_log_report：以yw_sys_number_talking_1h为主表，数据取的是360天数据，关联了相关 订单，渠道，员工，城市，带看等表，order_id不能为空。取的是最新的数据（使用了很多小时表）
-- ---------------------------------------------------------------------------------------
--  修改日志：
--  修改日期        修改人     修改内容
--  2020-07-06    蒋胜洋     1.渠道维度表由julive_dim.dim_clue_info改为julive_dim.dim_clue_base_info，订单数据更全
--                          2.添加字段from_source

set hive.execution.engine=spark;
set spark.app.name=fact_consultant_call_dtl;
set mapred.job.name=fact_consultant_call_dtl;
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


drop table if exists tmp_etl.tmp_fact_consultant_call_dtl;

create table tmp_etl.tmp_fact_consultant_call_dtl as
select
    t1.id as called_id, -- 主键id
    t1.recorder_id, -- 本次通话唯一标识
    t1.order_id as clue_id, -- 订单ID
    t1.employee_id, -- 咨询师ID
    t4.employee_name, -- 咨询师姓名
    t1.caller_type, -- 1:主叫人是咨询师，2:主叫人是用户
    t1.alerting_time, -- 响铃开始时间
    t1.connect_time, -- 按按钮时间
    t1.release_time, -- 开始通话时间
    t1.call_duration, -- 通话时长秒
    t1.bill_duration, -- 计费时长，秒
    t1.call_cost, -- 本次通话费用，元
    t1.call_result, -- 通话状态
    t1.caller_area, -- 主叫地区
    t1.called_area, -- 被叫地区

    if(t1.call_result = "ANSWERED",1,0) is_valid, -- 是否有效 1 有效，0 无效
    if(row_number() over(partition by t1.order_id order by t1.release_time asc)=1, 1, 0) as is_first_called, -- 是否是首电 1 是，0 否(全量数据)
    t8.distribute_time, -- 订单分配时间
    t8.distribute_date, -- 订单分配日期
    t8.city_id, -- 线索来源城市ID
    t8.city_name, -- 线索来源城市名称
    t8.city_seq, -- 带开城顺序的线索来源城市名称
    t8.customer_intent_city_id, -- 20191015 客户意向城市ID
    t8.customer_intent_city_name, -- 20191015 客户意向城市名称
    t8.customer_intent_city_seq, -- 20191015 带开城顺序客户意向城市名称
    t8.channel_id, -- 渠道ID
    t3.channel_name, -- 渠道名称
    t7.mgr_city,--20191231 主城
    t7.region, -- 20191220 所属大区
    t7.city_type, -- 20191220 城市类型
    t3.media_id, -- 媒体ID
    t3.media_name, -- 媒体名称
    t3.module_id, -- 模块ID
    t3.module_name, -- 模块名称
    t3.device_id, -- 设备ID
    t3.device_name, -- 设备名称

    t4.full_type, -- 转正类型
    t4.full_type_tc, -- 转正状态
    t4.entry_date, -- 入职日期
    t4.full_date, -- 转正日期
    t4.offjob_date, -- 离职日期
    coalesce(t4.direct_leader_id, t1.employee_manager_id) as direct_leader_id, -- 业务发生时员工直接上级id
    t4.team_leader_name, -- 主管
    t4.indirect_leader_id, -- 业务发生时员工直接上级的上级员工ID
    t4.p_leader_name, -- 经理
    t1.department_id, -- 部门ID
    t5.team_name as dept_name, -- 部门名称
    t8.project_type as project_type, -- 业态ID
    t8.project_type_name as project_type_name, -- 业态名称
    t8.interest_project as interest_project, -- 关注楼盘ID列表
    t8.intent_low_time as intent_low_time, -- 客户变为无意向时间
    to_date(t8.intent_low_time) as intent_low_date, -- 客户变为无意向日期
    t8.from_source, -- 数据来源: 1-居理数据 2-乌鲁木齐虚拟项目数据 3-二手房中介项目数据 4-加盟商数据
    from_unixtime(t1.create_datetime,'yyyy-MM-dd HH:mm:ss') as create_time -- 通话记录创建时间
from (
    select
        a.*
    from (
        select
            id, -- 主键id
            recorder_id, -- 本次通话唯一标识
            order_id, -- 订单ID
            employee_id, -- 咨询师ID
            caller_type, -- 1:主叫人是咨询师，2:主叫人是用户
            alerting_time, -- 响铃开始时间
            connect_time, -- 按按钮时间
            release_time, -- 开始通话时间
            call_duration, -- 通话时长秒
            bill_duration, -- 计费时长，秒
            call_cost, -- 本次通话费用，元
            call_result, -- 通话状态
            caller_area, -- 主叫地区
            called_area, -- 被叫地区
            employee_manager_id, -- 业务发生时员工直接上级id
            department_id, -- 部门ID
            create_datetime, -- 通话记录创建时间
            row_number() OVER (PARTITION BY recorder_id ORDER BY create_datetime DESC) as r -- 咨询师通话记录存在重复，要做去重
        from ods.yw_sys_number_talking      -- 咨询师通话记录
        where 1=1
        and call_result = "ANSWERED"     -- 通话状态
        and order_id != 0
        and order_id is not null
        and call_duration <= 7200
    ) a
    where a.r=1
) t1
left join julive_dim.dim_clue_base_info t8 on t1.order_id = t8.clue_id       -- 线索维度基表
left join julive_dim.dim_channel_info t3 on t8.channel_id = t3.channel_id     -- 渠道小时表
left join (
    -- 员工维度表
    select
        emp_id                as employee_id,       -- 雇员ID
        emp_name              as employee_name,     -- 雇员名称
        full_type             as full_type,         -- 转正类型
        full_type_tc          as full_type_tc,      -- 转正状态
        entry_date            as entry_date,        -- 入职日期
        full_date             as full_date,         -- 转正日期
        offjob_date           as offjob_date,       -- 离职日期
        direct_leader_id      as direct_leader_id,
        direct_leader_name    as team_leader_name,  -- 主管姓名
        indirect_leader_id    as indirect_leader_id,
        indirect_leader_name  as p_leader_name,     -- 经理姓名
        dept_id               as department_id,      -- 部门ID
        pdate
    from julive_dim.dim_ps_employee_info t  -- 自营员工维度快照表
) t4 on t1.employee_id = t4.employee_id and from_unixtime(t1.create_datetime, 'yyyyMMdd') = t4.pdate
left join (
    select
        department_id,
        team_name
    from (
        select
            department_id,
            team_name,
            row_number() over (partition by department_id order by effective_date desc) as rn
        from ods.yw_department_architecture_history
    ) a
    where rn = 1
) t5 on t4.department_id = t5.department_id     -- 部门信息表
-- 添加城市属性
left join julive_dim.dim_city t7 on t8.customer_intent_city_id = t7.city_id    -- 城市维表-自营
;


insert overwrite table julive_fact.fact_consultant_call_dtl
select
    called_id, -- 主键id
    recorder_id, -- 本次通话唯一标识
    clue_id, -- 订单ID
    employee_id, -- 咨询师ID
    employee_name, -- 咨询师姓名
    caller_type, -- 1:主叫人是咨询师，2:主叫人是用户
    alerting_time, -- 响铃开始时间
    connect_time, -- 按按钮时间
    release_time, -- 开始通话时间
    call_duration, -- 通话时长秒
    bill_duration, -- 计费时长，秒
    call_cost, -- 本次通话费用，元
    call_result, -- 通话状态
    caller_area, -- 主叫地区
    called_area, -- 被叫地区

    is_valid, -- 是否有效 1 有效，0 无效
    is_first_called, -- 是否是首电 1 是，0 否(全量数据)
    distribute_time, -- 订单分配时间
    distribute_date, -- 订单分配日期
    city_id, -- 线索来源城市ID
    city_name, -- 线索来源城市名称
    city_seq, -- 带开城顺序的线索来源城市名称
    customer_intent_city_id, -- 20191015 客户意向城市ID
    customer_intent_city_name, -- 20191015 客户意向城市名称
    customer_intent_city_seq, -- 20191015 带开城顺序客户意向城市名称
    channel_id, -- 渠道ID
    channel_name, -- 渠道名称
    mgr_city,--20191231 主城
    region, -- 20191220 所属大区
    city_type, -- 20191220 城市类型
    media_id, -- 媒体ID
    media_name, -- 媒体名称
    module_id, -- 模块ID
    module_name, -- 模块名称
    device_id, -- 设备ID
    device_name, -- 设备名称

    full_type, -- 转正类型
    full_type_tc, -- 转正状态
    entry_date, -- 入职日期
    full_date, -- 转正日期
    offjob_date, -- 离职日期
    direct_leader_id, -- 业务发生时员工直接上级id
    team_leader_name, -- 主管
    indirect_leader_id, -- 业务发生时员工直接上级的上级员工ID
    p_leader_name, -- 经理
    department_id, -- 部门ID
    dept_name, -- 部门名称
    project_type, -- 业态ID
    project_type_name, -- 业态名称
    interest_project, -- 关注楼盘ID列表
    intent_low_time, -- 客户变为无意向时间
    intent_low_date, -- 客户变为无意向日期
    from_source, -- 数据来源: 1-居理数据 2-乌鲁木齐虚拟项目数据 3-二手房中介项目数据 4-加盟商数据
    create_time, -- 通话记录创建时间
    current_timestamp as etl_time -- ETL跑数时间
from tmp_etl.tmp_fact_consultant_call_dtl
;


drop table if exists tmp_etl.tmp_fact_consultant_call_dtl;






