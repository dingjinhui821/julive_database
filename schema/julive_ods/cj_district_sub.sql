drop table if exists ods.cj_district_sub;
create external table ods.cj_district_sub(
id                                                     int             comment '主键',
cj_district_id                                         bigint          comment '城市表id',
cj_district_sub_id                                     bigint          comment '附属城市id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_district_sub'
;
