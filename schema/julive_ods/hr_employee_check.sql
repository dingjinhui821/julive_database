drop table if exists ods.hr_employee_check;
create external table ods.hr_employee_check(
id                                                     bigint          comment '考勤id',
employee_id                                            int             comment '员工id',
check_date                                             int             comment '考勤日期',
work_check_time                                        int             comment '上班打卡时间',
leave_check_time                                       int             comment '下班打卡时间',
plan_work_time                                         int             comment '排班上班时间',
plan_leave_time                                        int             comment '排卡下班时间',
status                                                 int             comment '考勤结果1正常 2外勤 3旷工 4迟到 5严重迟到 6早退 7休息 8休假 9迟到早退',
work_status                                            int             comment '上班打卡状态1正常 2未打卡 3外勤 4餐补(下班打卡时间超过晚上20:00) 5迟到 6严重迟到 7早退 8旷工(打卡时间-排班打卡时间>4小时) 9休假 10休息',
leave_status                                           int             comment '下班打卡状态1正常 2未打卡 3外勤 4餐补(下班打卡时间超过晚上20:00) 5迟到 6严重迟到 7早退 8旷工(打卡时间-排班打卡时间>4小时) 9休假 10休息',
leave_work_days                                        double          comment '旷工天数',
over_work_days                                         double          comment '加班天数',
not_check_times                                        int             comment '缺卡次数',
come_late_times                                        int             comment '迟到次数',
need_check_days                                        double          comment '应打卡天数',
leave_early_times                                      int             comment '早退次数',
personal_leave_days                                    double          comment '事假天数',
sick_leave_days                                        double          comment '病假天数',
bereave_leave_days                                     double          comment '丧假天数',
wedding_leave_days                                     double          comment '婚假天数',
meal_supply_times                                      int             comment '餐补次数',
maternity_leave_days                                   double          comment '产假天数',
paternity_leave_days                                   double          comment '陪产假天数',
annual_leave_days                                      double          comment '休年假天数',
tune_off_days                                          double          comment '调休天数',
over_work_type                                         int             comment '加班类型 1上午 2下午 3全天',
check_month                                            int             comment '考勤月份',
can_approval                                           int             comment '能否发起审批 1可以 2不可以',
approval_time_type                                     int             comment '审批时段 1上午 2下午 3全天',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
paid_vacation_days                                     double          comment '带薪假',
injury_days                                            double          comment '工伤假',
check_up_days                                          double          comment '产检假',
out_days                                               double          comment '外出天数',
evection_days                                          double          comment '出差天数',
am_appeal                                              int             comment '上午能否发起申诉，1能 2不能',
pm_appeal                                              int             comment '下午能否发起申诉，1能 2不能',
need_attendance_days                                   double          comment '应出勤天数',
serious_come_late_times                                int             comment '严重迟到次数',
effective_outside_days                                 double          comment '有效外勤',
vacation_type                                          int             comment '休假类型',
work_location_result                                   string          comment '上班打卡位置状态',
leave_location_result                                  string          comment '下班打卡位置状态',
appeal_work_status                                     int             comment '申诉上班考勤状态',
appeal_leave_status                                    int             comment '申诉下班考勤状态',
remark                                                 string          comment '钉钉考勤中的备注',
is_modify                                              int             comment '人力是否修改过考勤数据 1是 2否',
is_full_work                                           int             comment '是否有全勤奖 1 是 2 否',
outside_times                                          int             comment '外勤次数',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
post_id                                                int             comment '岗位id',
employee_status                                        int             comment '员工状态:0离职，1在职，2停访',
full_type                                              int             comment '转正状态 1未转正 2已转正',
extra_need_attendance_days                             double          comment '应出勤天数（仅供订单侧使用，其它端使用请联系人力侧）',
is_schedule                                            int             comment '当天是否有排班1 有 2无',
config_id                                              int             comment '考勤配置id',
config_condition                                       string          comment '配置条件',
adjust_city_id                                         bigint          comment '业绩核算城市',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_employee_check'
;
