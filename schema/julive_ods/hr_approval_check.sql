drop table if exists ods.hr_approval_check;
create external table ods.hr_approval_check(
id                                                     int             comment '',
approval_id                                            int             comment '审批id hr_approval_apply 审批申请公共信息表id',
appeal_check_data                                      string          comment '申诉考勤日期及阶段json（日期，阶段，考勤id，考勤状态）',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
reason                                                 string          comment '申请理由',
approval_examiner                                      int             comment '被审批人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_approval_check'
;
