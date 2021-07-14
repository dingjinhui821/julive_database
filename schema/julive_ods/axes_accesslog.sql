drop table if exists ods.axes_accesslog;
create external table ods.axes_accesslog(
id                                                     int             comment '',
user_agent                                             string          comment '',
ip_address                                             string          comment '',
username                                               string          comment '',
trusted                                                int             comment '',
http_accept                                            string          comment '',
path_info                                              string          comment '',
attempt_time                                           string          comment '',
logout_time                                            string          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/axes_accesslog'
;
