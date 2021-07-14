drop table if exists ods.yw_front_website_recommend_project_record;
create external table ods.yw_front_website_recommend_project_record(
id                                                     int             comment '',
website_type                                           string          comment '网站类型',
page_name                                              string          comment '页面名称',
position                                               string          comment '位置:页面上的某个位置',
identity                                               string          comment '标识:页面、位置组成的标识，以“_”连接',
project_num_limit                                      int             comment '楼盘个数限制',
project_id_list                                        string          comment '楼盘id，多个用“，”隔开',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
description                                            string          comment '页面描述',
city_id                                                bigint          comment '城市id',
district_id                                            bigint          comment '区域',
status                                                 int             comment '1: 显示2:隐藏',
pic                                                    string          comment '图标',
abtest_project_id_list                                 string          comment '狙击手楼盘id，多个用“，”隔开',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_front_website_recommend_project_record'
;
