drop table if exists ods.yw_material_report_template;
create external table ods.yw_material_report_template(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
user_id                                                bigint          comment '用户id',
user_name                                              string          comment '用户名',
employee_id                                            bigint          comment '员工id',
employee_name                                          string          comment '员工名称',
business_id                                            bigint          comment '业务id 联系id\带看id',
follow_type                                            int             comment '1是联系2是带看',
status                                                 int             comment '状态（删除-1,有效未发送=1,已发送=2）',
like_num                                               int             comment '点赞数量',
spare_mobile                                           string          comment '其他手机号',
mobile                                                 string          comment '手机号',
creator                                                int             comment '创建人id',
updator                                                int             comment '更新人id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
push_datetime                                          int             comment '推送时间',
views                                                  int             comment '浏览次数',
first_push_datetime                                    int             comment '报告首次发送时间',
url                                                    string          comment '报告发送地址',
is_import                                              int             comment '是否已导入到楼市头条 0 未导入   1 已导入',
city_id                                                bigint          comment '城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_material_report_template'
;
