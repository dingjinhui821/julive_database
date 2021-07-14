drop table if exists ods.cj_abtest_variation_relation;
create external table ods.cj_abtest_variation_relation(
id                                                     int             comment '主键',
experiment_id                                          string          comment '实验变量id',
variation_id                                           string          comment '实验变量版本id',
variation                                              int             comment '版本 0:original_version，1:v1 ...',
url                                                    string          comment '多链接url',
md5_url                                                string          comment '多链接url的md5值',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
checked                                                int             comment '多链接选中状态:0:未选中，1选中',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_abtest_variation_relation'
;
