drop table if exists ods.cj_project_sell_point;
create external table ods.cj_project_sell_point(
id                                                     bigint          comment '',
project_id                                             bigint          comment '楼盘id',
sell_point                                             string          comment '卖点',
type                                                   int             comment '1:列表页卖点 2:详情页卖点',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_sell_point'
;
