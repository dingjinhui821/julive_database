drop table if exists ods.yw_employee_grade_bonus;
create external table ods.yw_employee_grade_bonus(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
employee_name                                          string          comment '员工姓名',
job_number                                             string          comment '员工工号',
employee_id                                            int             comment '员工id',
department_id                                          int             comment '部门id',
team_leader_id                                         int             comment '主管id',
y_value                                                double          comment '城市y值',
commission_type                                        int             comment '佣金类型(1:网签 2:认购)',
sign_contract_commission                               double          comment '奖金基数值之和',
actual_attendance_days_sum                             int             comment '实际出勤天数总和',
actual_level                                           int             comment '实际达际等级（发钱等级）',
quarter_grade                                          int             comment '季度等级',
standard_grader                                        int             comment '本月达标等级(结算月等级)',
grade_bonus                                            double          comment '职级奖金',
grade_ratio                                            double          comment '职级奖金比例',
adjust_datetime                                        int             comment '核算时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
full_type                                              int             comment '1 未转正 2已转正',
should_attendance_days_detail                          string          comment '当月出勤天数详情',
contract_commission_detail                             string          comment '职级签约奖金基数值详情',
employee_status                                        int             comment '员工状态:0离职，1在职，2停访',
post_id                                                int             comment '岗位id',
type                                                   int             comment '职级类型1:核算月职级奖金 2:差值奖金',
diff_types                                             string          comment '职级差值类型 10:月报更新',
actual_grade_bonus                                     double          comment '职级奖金差额',
update_log                                             string          comment '日志',
content                                                string          comment '职级差额展示文本',
actual_adjust_datetime                                 int             comment '差额核算时间',
adjust_city_id                                         bigint          comment '核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_grade_bonus'
;
