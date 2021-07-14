drop table if exists ods.yw_company_pos;
create external table ods.yw_company_pos(
id                                                     int             comment '',
city_id                                                int             comment '',
name                                                   string          comment '',
lat                                                    string          comment '',
lng                                                    string          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                int             comment '',
updator                                                int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_company_pos'
;
