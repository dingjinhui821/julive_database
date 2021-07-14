drop table if exists ods.cj_project_match_result;
create external table ods.cj_project_match_result(
id                                                     bigint          comment '主键id',
city_id                                                int             comment '城市id',
cookie                                                 string          comment '用户标识',
sort_json                                              string          comment '匹配度排序结果json',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_match_result'
;
