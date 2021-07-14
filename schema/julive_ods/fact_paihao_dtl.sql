drop table if exists julive_fact.fact_paihao_dtl;
create external table julive_fact.fact_paihao_dtl(
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/fact_paihao_dtl'
;
