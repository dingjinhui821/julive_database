drop table if exists ods.ex_project_cooperation_record;
create external table ods.ex_project_cooperation_record(
id                                                     bigint          comment 'id',
project_id                                             bigint          comment '楼盘id',
employee_id                                            int             comment '跟进人',
record_datetime                                        int             comment '记录日期',
record_year                                            int             comment '记录年',
record_quarterly                                       int             comment '记录季度',
record_month                                           int             comment '记录月',
is_direct_sign                                         int             comment '是否是直签 1是2否',
is_platform_sign                                       int             comment '是否是平台签 1是2否',
total_direct_sign_days                                 int             comment '总计直签天数',
total_platform_sign_days                               int             comment '总计平台签天数',
is_direct_sign_record_point                            int             comment '是否是直签记录点1是0否',
is_platform_sign_record_point                          int             comment '是否是平台签记录点1是0否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
project_city_id                                        int             comment '楼盘地理城市',
contract_id                                            int             comment '合同id',
partner_id                                             int             comment '合作方',
cooperate_type                                         int             comment '合作方式',
direct_sign_type                                       int             comment '直签类型',
contract_follow_employee                               int             comment '合同跟进人',
contract_begin_datetime                                int             comment '合同开始时间',
contract_end_datetime                                  int             comment '合同结束时间',
contract_file_datetime                                 int             comment '合同归档时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_project_cooperation_record'
;
