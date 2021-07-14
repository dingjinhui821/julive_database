drop table if exists ods.cj_project_dynamic_info;
create external table ods.cj_project_dynamic_info(
id                                                     bigint          comment '',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
project_id                                             bigint          comment '楼盘id',
status                                                 int             comment '状态:1，正常，-1，删除',
date                                                   int             comment '日期',
title                                                  string          comment '标题',
content                                                string          comment '描述',
date_str                                               string          comment '',
source                                                 int             comment '来源,99为侃家',
review                                                 int             comment '0,未审核，1，通过，2，不通过',
author                                                 bigint          comment '审核者',
image_list                                             string          comment '图片集合',
main_text                                              string          comment '正文',
relation_house_info                                    string          comment '关联房源信息',
relaton_family_unit                                    string          comment '关联户型',
template_id                                            int             comment '留电模板id',
headline_child_type                                    string          comment '存储导入楼市头条的子类别，与头条表cj_project_headline的child_type相同(10,20,30,90)',
city_id                                                int             comment '城市id',
is_system_add                                          string          comment '是否是系统添加的售馨动态 1是 2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_dynamic_info'
;
