drop table if exists ods.cj_layout_diagram_history;
create external table ods.cj_layout_diagram_history(
id                                                     bigint          comment '',
layout_diagram_id                                      bigint          comment '户型图id',
summary                                                string          comment '说明',
image_url                                              string          comment '图片地址',
project_id                                             bigint          comment '楼盘id',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
update_party                                           int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_layout_diagram_history'
;
