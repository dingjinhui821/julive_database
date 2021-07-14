drop table if exists ods.useradmin_userprofile;
create external table ods.useradmin_userprofile(
home_directory                                         string          comment '',
id                                                     int             comment '',
user_id                                                int             comment '',
creation_method                                        string          comment '',
first_login                                            int             comment '',
last_activity                                          string          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/useradmin_userprofile'
;
