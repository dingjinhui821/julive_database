drop table if exists ods.cj_special_house_notify_info;
create external table ods.cj_special_house_notify_info(
id                                                     int             comment '主键id',
project_id                                             int             comment '楼盘id',
`desc`                                                 string          comment '特价房描述',
end_datetime_specific                                  int             comment '结束时间',
detail_list                                            string          comment '特价房列表详情',
img_list                                               string          comment '特价房图片',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_special_house_notify_info'
;
