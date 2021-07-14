set hive.execution.engine=spark;
set spark.app.name=dim_dsp_account_rebate;
set spark.yarn.queue=etl;
set spark.executor.cores=1;
set spark.executor.memory=2g;
set spark.executor.instances=1;


-- set etl_date = '${hiveconf:etlDate}';
-- set etl_yestoday = '${hiveconf:etlYestoday}'; 
-- set etl_date = '20210101';
-- set etl_date = regexp_replace(date_add(current_date(),-1),'-',''); -- 仅用于测试 
-- set etl_yestoday = regexp_replace(date_add(current_date(),-2),'-',''); -- 仅用于测试 

--   功能描述：渠道返点维度表
--   输 入 表 ：
--       ods.dsp_account_rebate            -- 渠道返点表

--   输 出 表：julive_dim.dim_dsp_account_rebate
-- 
--   创 建 者： 薛理  15996981324
--   创建日期： 2021/06/03 14:16

insert overwrite table julive_dim.dim_dsp_account_rebate  
select 
    id,
    dsp_account_id,
    rebate_timestamp,
    rebate_date,
    start_date,
    end_date,
    rebate,
    is_repair_history,
    current_timestamp() as etl_time
from (
    select
        id,
        dsp_account_id,
        unix_timestamp(date_add(from_unixtime(start_date,'yyyy-MM-dd'),i),'yyyy-MM-dd') as rebate_timestamp,
        date_add(from_unixtime(start_date,'yyyy-MM-dd'),i) as rebate_date,
        from_unixtime(start_date,'yyyy-MM-dd') as start_date,
        from_unixtime(end_date,'yyyy-MM-dd') as end_date,
        rebate,
        is_repair_history,
        row_number() over (partition by dsp_account_id,date_add(from_unixtime(start_date,'yyyy-MM-dd'),i) order by start_date desc) as rn        
    from ods.dsp_account_rebate
    lateral view posexplode(split(space(datediff(from_unixtime(end_date,'yyyy-MM-dd'),from_unixtime(start_date,'yyyy-MM-dd'))),'')) pe as i,x
    where status = 1
) as tab where rn = 1;

