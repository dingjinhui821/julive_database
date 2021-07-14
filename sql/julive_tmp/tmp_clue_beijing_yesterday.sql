set hive.execution.engine=spark;
set spark.app.name=tmp_clue_beijing_yesterday;  
set spark.yarn.queue=etl;
set spark.executor.cores=2;
set spark.executor.memory=2g;
set spark.executor.instances=12;
set spark.yarn.executor.memoryOverhead=1024;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 取昨天北京线索数量
insert overwrite table tmp_bi.tmp_clue_beijing_yesterday partition(pdate) 
select 
    clue_id,     -- 订单id
    default.decode_item(user_mobile) as user_mobile, -- 手机号
    create_date,
    FROM_UNIXTIME(UNIX_TIMESTAMP(date_add(current_date(),-1),'yyyy-MM-dd'),'yyyyMMdd') as pdate
from julive_dim.dim_clue_base_info  
where 1=1 
and create_date=date_add(current_date(),-1)
and customer_intent_city_name='北京'
;

