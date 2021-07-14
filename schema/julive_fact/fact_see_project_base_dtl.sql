drop table if exists julive_fact.fact_see_project_base_dtl;
create external table julive_fact.fact_see_project_base_dtl(
see_project_id                                    bigint                  comment '带看楼盘ID',
see_id                                            bigint                  comment '带看ID',
see_create_date                                   string                  comment '带看日期',
clue_id                                           bigint                  comment '线索维度表ID',
org_id                                            int                     comment '公司id',
org_type                                          int                     comment '类型 1 居理 2加盟',
org_name                                          string                  comment '公司名称',
channel_id                                        bigint                  comment '渠道ID',
city_id                                           int                     comment '线索来源城市ID',
city_name                                         string                  comment '线索来源城市名称',
city_seq                                          string                  comment '带开城顺序的线索来源城市名称',
customer_intent_city_id                           int                     comment '客户意向城市ID',
customer_intent_city_name                         string                  comment '客户意向城市名称',
customer_intent_city_seq                          string                  comment '带开城顺序的客户意向城市名称',
source                                            int                     comment '订单了解途径',
source_tc                                         string                  comment '用户来源,存储source转码结果',
region                                            string                  comment '带看楼盘所在大区',
mgr_city_id                                       string                  COMMENT '带看楼盘所在城市主城id', 
mgr_city                                          string                  COMMENT '带看楼盘所在城市主城名称', 
project_city_id                                   int                     comment '带看楼盘所在城市ID',
project_city_name                                 string                  comment '带看楼盘所在城市名称',
project_city_seq                                  string                  comment '带开城顺序的带看楼盘所在城市名称',

emp_region                                        string                  comment '员工所在大区',
emp_mgr_city_seq                                  string                  comment '员工所在城市主城seq',
emp_mgr_city                                      string                  comment '员工所在主城名称',
emp_city_id                                       string                  comment '员工所在主城id',
emp_city_name                                     string                  comment '员工所在城市名称',
emp_city_seq                                      string                  comment '带开城顺序的员工所在城市名称',

project_id                                        bigint                  comment '楼盘维度表ID',
project_name                                      string                  comment '楼盘名称',
clue_emp_id                                       bigint                  comment '当前上户人员工ID，引用员工维度',
clue_emp_realname                                 string                  comment '当前上户人员工姓名',
see_emp_id                                        bigint                  comment '带看人的员工ID，引用员工维度',
see_emp_realname                                  string                  comment '带看人的员工姓名',
invitation_emp_id                                 bigint                  comment '邀约人ID，引用员工维度',
invitation_emp_realname                           string                  comment '邀约人的员工姓名',
didi_emp_id                                       int                     comment '打车人员工ID，引用员工维度',
didi_emp_name                                     int                     comment '打车人员工姓名',
user_id                                           bigint                  comment '用户ID',
user_name                                         string                  comment '用户姓名',
plan_real_begin_time                              string                  comment '计划/实际 带看开始时间',
plan_to_real_time                                 string                  comment '预约转实际时间',
plan_real_end_time                                string                  comment '计划/实际 带看结束时间',
status                                            int                     comment '带看状态 10:已预约，并自己带看；20:已预约，并且邀请其他咨询师带看；30:已预约，并且其他咨询师同意带看；40:已带看； 45:已发送邀评带看的短信  50:已评价   60：取消带看',
merg_tag                                          int                     comment '合并同组带看记录的id：0默认不合并，大于0表示被合并',
see_project_type                                  int                     comment '带看类型 1普通带看 2关键带看 3无效带看',
audit_status                                      int                     comment '关键审核状态 1未处理 2处理中 3通过 4不通过',
follow_tags                                       string                  comment '跟进频率标签',
sure_method                                       int                     comment '确认途径，1电话 2短信 3微信',
is_first_visit                                    int                     comment '是否首访，或同盘首访或同盘复访:1首访2同盘复访3非同盘复访',
see_type                                          int                     comment '1-线下带看 2-线上带看',
have_confirmation_sheet                           int                     COMMENT '是否有客户确认单',
is_first_see                                      int                     comment '是否首复访:1 是 0 否',
is_first_see_project                              int                     comment '是否线索-楼盘首复访:1 是 0 否',
orig_see_num                                      int                     comment '原带看量：1 或 0',
orig_see_project_num                              int                     comment '原带看楼盘量：1',
see_num                                           int                     comment '有效带看量：1 或 0',
see_project_num                                   int                     comment '有效带看楼盘量：1',
from_source                                       int                     comment '1-自营项目 2-乌鲁木齐项目 3-二手房中介项目 4-加盟商项目',
see_create_time                                   string                  comment '带看创建日期',
create_time                                       string                  comment '带看楼盘创建日期',
etl_time                                          string                  comment 'ETL跑数时间'
) comment '带看主题事实表' 
stored as parquet 
;



-- 子表建表语句 
drop table if exists julive_fact.fact_see_project_dtl;
create table julive_fact.fact_see_project_dtl like julive_fact.fact_see_project_base_dtl;

drop table if exists julive_fact.fact_wlmq_see_project_dtl;
create table  julive_fact.fact_wlmq_see_project_dtl like julive_fact.fact_see_project_base_dtl;

drop table if exists julive_fact.fact_esf_see_project_dtl;
create table  julive_fact.fact_esf_see_project_dtl like julive_fact.fact_see_project_base_dtl;

drop table if exists julive_fact.fact_jms_see_project_dtl;
create table  julive_fact.fact_jms_see_project_dtl like julive_fact.fact_see_project_base_dtl;


