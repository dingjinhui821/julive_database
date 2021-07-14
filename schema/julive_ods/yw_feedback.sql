drop table if exists ods.yw_feedback;
create external table ods.yw_feedback(
id                                                     bigint          comment '',
city_id                                                int             comment '城市id',
employee_id                                            int             comment '咨询师id',
employee_realname                                      string          comment '咨询师姓名',
job_number                                             int             comment '工号',
employee_role                                          string          comment '角色',
message                                                string          comment '建议',
problems                                               string          comment '问题',
create_datetime                                        int             comment '提交时间',
update_datetime                                        int             comment '更新时间',
`from`                                                 int             comment '来源，1backend添加，2咨询师app添加',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_feedback'
;
