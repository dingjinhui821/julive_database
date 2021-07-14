drop table if exists ods.desktop_document2;
create external table ods.desktop_document2(
id                                                     int             comment '',
owner_id                                               int             comment '',
name                                                   string          comment '',
description                                            string          comment '',
uuid                                                   string          comment '',
type                                                   string          comment '',
data                                                   string          comment '',
extra                                                  string          comment '',
last_modified                                          string          comment '',
version                                                int             comment '',
is_history                                             int             comment '',
parent_directory_id                                    int             comment '',
search                                                 string          comment '',
is_managed                                             int             comment '',
is_trashed                                             int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/desktop_document2'
;
