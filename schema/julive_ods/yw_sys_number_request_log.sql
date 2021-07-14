drop table if exists julive_bak.yw_sys_number_request_log;
create external table julive_bak.yw_sys_number_request_log(
id                                                     int             comment '',
business_id                                            string          comment '通话(recorder_id),和短信(sms_id)的唯一id',
create_datetime                                        string          comment '请求时间',
request                                                string          comment '请求的参数，json',
response                                               string          comment '返回的数据，json',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_sys_number_request_log'
;
