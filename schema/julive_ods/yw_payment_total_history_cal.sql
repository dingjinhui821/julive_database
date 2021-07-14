drop table if exists ods.yw_payment_total_history_cal;
create external table ods.yw_payment_total_history_cal(
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_payment_total_history_cal'
;
