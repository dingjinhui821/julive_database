drop table if exists ods.hr_offjob_element;
create external table ods.hr_offjob_element(
id                                                     int             comment '自增id',
element_name                                           string          comment '离职因素',
element_type                                           int             comment '离职因素类型 主要离职因素1，主要离职因素2，主要离职因素3，次要离职因素4',
is_delete                                              int             comment '是否删除1是 2 否',
is_use                                                 int             comment '是否使用1使用 2未使用',
level_id                                               int             comment '层级',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_offjob_element'
;
