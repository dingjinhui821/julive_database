drop table if exists ods.cj_case_field_channel;
create external table ods.cj_case_field_channel(
id                                                     bigint          comment '',
case_field_id                                          int             comment '案场id',
company_id                                             int             comment '渠道公司id',
visit_num                                              double          comment '到访量',
paihao_num                                             double          comment '排号量',
subscribe_num                                          double          comment '认购量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_case_field_channel'
;
