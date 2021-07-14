set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert overwrite table ods.ods_event_archive partition(pdate) 
select json,pdate 
from ods.ods_event_realtime 
where pdate = regexp_replace(date_add(current_date(),-2),'-','') -- 同步前天数据 
;


