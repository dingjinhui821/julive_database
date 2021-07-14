drop table if exists ods.cj_sa_comjia_uid_map;
create external table ods.cj_sa_comjia_uid_map(
sa_uid                                                 string          comment '神策用户id',
comjia_uid                                             bigint          comment '侃家用户id',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_sa_comjia_uid_map'
;
