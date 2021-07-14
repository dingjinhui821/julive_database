drop table if exists julive_app.app_emp_city_first_seetype_day_base;
CREATE TABLE julive_app.app_emp_city_first_seetype_day_base(
date_str                          string COMMENT '日期字符串:yyyy-MM-dd', 
week_type                         string COMMENT '周中 周末', 
city_id                           bigint COMMENT '城市ID', 
city_name                         string COMMENT '城市名称', 
city_seq                          string COMMENT '带开城顺序的城市名称', 
city_region                       string COMMENT '所属大区', 
city_type                         string COMMENT '新老城 老城含副区', 
mgr_city                          string COMMENT '主城', 
emp_id                            int    COMMENT '员工id', 
emp_name                          string COMMENT '员工姓名', 
first_see_type                    string COMMENT '首复访类型', 
direct_leader_id                  int    COMMENT '业务发生时员工直接上级id', 
direct_leader_name                string COMMENT '业务发生时员工直接上级id', 
indirect_leader_id                int    COMMENT '业务发生时员工直接上级的上级员工ID', 
indirect_leader_name              string COMMENT '业务发生时员工直接上级的上级员工姓名', 
now_direct_leader_id              int    COMMENT '当前员工直接上级id', 
now_direct_leader_name            string COMMENT '当前员工直接上级id', 
now_indirect_leader_id            int    COMMENT '当前员工直接上级的上级员工ID', 
now_indirect_leader_name          string COMMENT '当前员工直接上级的上级员工姓名', 
full_type                         int    COMMENT '转正状态 1未转正 2已转正', 
full_type_tc                      string COMMENT '转正状态 1未转正 2已转正', 
see_num                           int    COMMENT '带看量', 
subscribe_contains_cancel_ext_num int    COMMENT '认购量:含退 含外联', 
from_source                       int           COMMENT '1-自营2乌鲁木齐3二手房中介4-加盟商', 
org_id                            int           COMMENT '公司id',
org_name                          string        COMMENT '公司名称', 
emp_city_id                       string        COMMENT '咨询师城市ID',
emp_city_name                     string        COMMENT '咨询师城市名称',
emp_city_seq                      string        COMMENT '带开城顺序的员工所在城市名称',
emp_city_see_num                  int           COMMENT '带看楼盘量按咨询师统计',
emp_city_subscribe_contains_cancel_ext_num   int           COMMENT '认购量:含退 含外联按咨询师统计',
etl_time string COMMENT 'ETL跑数时间')
COMMENT '日-咨询师-城市-首复访报表'
stored as parquet;



drop table if exists julive_app.app_emp_city_first_seetype_day;
create table julive_app.app_emp_city_first_seetype_day like julive_app.app_emp_city_first_seetype_day_base;

drop table if exists julive_app.app_wlmq_emp_city_first_seetype_day;
create table  julive_app.app_wlmq_emp_city_first_seetype_day like julive_app.app_emp_city_first_seetype_day_base;

drop table if exists julive_app.app_esf_emp_city_first_seetype_day;
create table  julive_app.app_esf_emp_city_first_seetype_day like julive_app.app_emp_city_first_seetype_day_base;

drop table if exists julive_app.app_jms_emp_city_first_seetype_day;
create table  julive_app.app_jms_emp_city_first_seetype_day like julive_app.app_emp_city_first_seetype_day_base;
