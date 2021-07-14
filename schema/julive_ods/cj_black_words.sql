drop table if exists ods.cj_black_words;
create external table ods.cj_black_words(
id                                                     int             comment 'id',
name                                                   string          comment '',
city_id                                                int             comment '城市id',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
word_type                                              int             comment '1来自用户点评',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_black_words'
;
