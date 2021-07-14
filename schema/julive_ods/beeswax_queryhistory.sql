drop table if exists ods.beeswax_queryhistory;
create external table ods.beeswax_queryhistory(
submission_date                                        string          comment '',
last_state                                             int             comment '',
server_id                                              string          comment '',
log_context                                            string          comment '',
design_id                                              int             comment '',
owner_id                                               int             comment '',
query                                                  string          comment '',
has_results                                            int             comment '',
id                                                     int             comment '',
notify                                                 int             comment '',
server_name                                            string          comment '',
server_host                                            string          comment '',
server_port                                            int             comment '',
server_type                                            string          comment '',
server_guid                                            string          comment '',
operation_type                                         int             comment '',
modified_row_count                                     double          comment '',
statement_number                                       int             comment '',
query_type                                             int             comment '',
is_redacted                                            int             comment '',
extra                                                  string          comment '',
is_cleared                                             int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/beeswax_queryhistory'
;
