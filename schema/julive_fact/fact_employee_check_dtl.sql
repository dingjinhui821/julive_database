drop table if exists julive_fact.fact_employee_check_base_dtl;
CREATE TABLE julive_fact.fact_employee_check_base_dtl(
emp_check_id       bigint COMMENT '考勤id', 
emp_id             bigint COMMENT '员工id', 
emp_name           string COMMENT '员工ID', 
job_number         bigint COMMENT '员工工号', 
post_id            bigint COMMENT '岗位ID', 
post_name          string COMMENT '岗位名称', 
entry_date         string COMMENT '入职日期', 
dept_id            bigint COMMENT '部门ID', 
dept_name          string COMMENT '部门名称', 
city_id            bigint COMMENT '城市ID', 
city_name          string COMMENT '城市名称', 
city_seq           string COMMENT '带开城顺序的城市名称', 
check_date         string COMMENT '考勤日期:yyyy-MM-dd', 
weekday_zh         string COMMENT '星期中文:[星期一..星期日]', 
work_check_time    string COMMENT '上班打卡时间:yyyy-MM-dd HH:mm:ss', 
leave_check_time   string COMMENT '下班打卡时间:yyyy-MM-dd HH:mm:ss', 
plan_work_time     string COMMENT '排班上班时间:yyyy-MM-dd HH:mm:ss', 
plan_leave_time    string COMMENT '排卡下班时间:yyyy-MM-dd HH:mm:ss', 
status             int COMMENT '考勤结果:1正常 2外勤 3旷工 4迟到 5严重迟到 6早退 7休息 8休假 9迟到早退', 
plan_workday_num   float COMMENT '法定工作天数', 
real_workday_num   float COMMENT '出勤天数', 
from_source        int  COMMENT '1-自营 2-乌鲁木齐 3-二手房中介',
etl_time           string COMMENT 'ETL跑数时间')
COMMENT '员工考勤明细事实表'
stored as parquet;

drop table if exists julive_fact.fact_esf_employee_check_dtl;
create external table julive_fact.fact_esf_employee_check_dtl 
like julive_fact.fact_employee_check_base_dtl;

drop table if exists julive_fact.fact_wlmq_employee_check_dtl;
create external table julive_fact.fact_wlmq_employee_check_dtl 
like julive_fact.fact_employee_check_base_dtl;

drop table if exists julive_fact.fact_employee_check_dtl;
create external table julive_fact.fact_employee_check_dtl 
like julive_fact.fact_employee_check_base_dtl;

drop table if exists julive_fact.fact_jms_employee_check_dtl;
create external table julive_fact.fact_jms_employee_check_dtl 
like julive_fact.fact_employee_check_base_dtl;
