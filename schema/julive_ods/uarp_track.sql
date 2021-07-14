drop table if exists julive_ods.uarp_track;
create external table julive_ods.uarp_track(
id                                                     int             comment '自增id',
product_id                                             int             comment '项目id',
tag                                                    int             comment '产品自定义名称',
version                                                int             comment '埋点版本id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
is_delete                                              int             comment '是否被逻辑删除
0:未删除，1:删除',
creator                                                int             comment '创建者',
unique_id                                              string          comment '埋点的所有字段的md5值',
product_id_alias                                       int             comment 'product_id的别名，比如安卓和ios的埋点是同一个，但是要传两个product_id',
unique_id_alias                                        string          comment 'md5值的别名',
comment                                                string          comment '备注',
status                                                 int             comment '审核状态（0待审核，1已审核，-1已舍弃）',
updater                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/uarp_track'
;
