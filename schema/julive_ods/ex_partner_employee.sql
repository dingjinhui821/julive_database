drop table if exists ods.ex_partner_employee;
create external table ods.ex_partner_employee(
id                                                     int             comment '开发商员工id',
name                                                   string          comment '姓名',
post_name                                              string          comment '岗位',
partner_id                                             string          comment '归属公司id，逗号分隔',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
weixin_number                                          string          comment '微信号',
manage_scope                                           string          comment '权责',
portrait_description                                   string          comment '肖像描述',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_partner_employee'
;
