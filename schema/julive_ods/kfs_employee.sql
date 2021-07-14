drop table if exists ods.kfs_employee;
create external table ods.kfs_employee(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
employee_name                                          string          comment '员工姓名',
job_number                                             string          comment '登录手机号',
mobile                                                 string          comment '手机号',
qr_code_img                                            string          comment '二维码图片',
image                                                  string          comment '形象照片',
delete_time                                            int             comment '删除时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
password                                               string          comment '密码',
auth_key                                               string          comment '验证',
status                                                 int             comment '状态:0删除，1正常，2禁用',
password_safety                                        int             comment '密码是否安全',
can_alloc                                              int             comment '是否能接单:1是2否',
is_consultant                                          int             comment '是否居理咨询师1是2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_employee'
;
