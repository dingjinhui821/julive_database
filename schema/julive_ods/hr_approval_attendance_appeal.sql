drop table if exists ods.hr_approval_attendance_appeal;
create external table ods.hr_approval_attendance_appeal(
id                                                     int             comment 'id',
approval_id                                            int             comment '审批id',
employee_id                                            bigint          comment '员工id',
apply_time                                             int             comment '日期 （0点时间戳）',
check_type                                             int             comment '打卡类型:1:上班 2:下班',
check_time                                             string          comment '打卡时间',
check_status                                           int             comment '打卡状态 1:当日 2:次日',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_approval_attendance_appeal'
;
