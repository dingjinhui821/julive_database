drop table if exists ods.hj_keyword_adjust;
create external table ods.hj_keyword_adjust(
id                                                     int             comment 'id',
media_type                                             int             comment '媒体类型（百度:1360:2搜狗:3今日头条:4）',
account                                                string          comment '账户',
account_id                                             int             comment '账户在dsp_account表里对应的id',
adjust_time                                            int             comment '调整时间,用0点的时间戳',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_keyword_adjust'
;
