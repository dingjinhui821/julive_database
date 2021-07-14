drop table if exists ods.cj_project_rank;
create external table ods.cj_project_rank(
id                                                     bigint          comment 'id',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新者',
update_datetime                                        int             comment '更新时间',
project_id                                             bigint          comment '楼盘id',
city_id                                                int             comment '城市id',
district_id                                            int             comment '区域id',
rank_type                                              int             comment '1:人气榜 2:热销榜3:热搜榜',
rank_value                                             int             comment '人气榜-uv、热搜榜-uv、热销榜-认购量',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_rank'
;
