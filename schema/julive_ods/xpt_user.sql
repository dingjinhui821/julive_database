drop table if exists ods.xpt_user;
create external table ods.xpt_user(
id                                                     int             comment '自增id',
user_name                                              string          comment '用户名字',
mobile                                                 string          comment '手机号',
password                                               string          comment '密码',
update_password_time                                   int             comment '更新密码时间',
status                                                 int             comment '状态:在职 1  离职2',
offjob_datetime                                        int             comment '离职时间',
is_julive                                              int             comment '是否是居理账户 1是 2否',
julive_employee_id                                     bigint          comment '居理用户id',
id_card                                                string          comment '身份证号码',
portrait                                               string          comment '职业照片',
wechat_code_url                                        string          comment '微信二维码',
customer_count                                         int             comment '服务客户数量',
practice_start_year                                    int             comment '从业年份',
role_name                                              string          comment '角色',
wxc_user_id                                            int             comment '经纪人关注服务号对应的id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_user'
;
