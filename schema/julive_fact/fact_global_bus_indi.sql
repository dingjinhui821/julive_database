drop table if exists julive_fact.fact_global_bus_indi;
create table julive_fact.fact_global_bus_indi(
global_id                 string  comment '主键',
julive_id                 string  comment '用户id',
user_name                 string  comment '用户名',
user_mobile               string  comment '用户手机号',
emp_id                    string  comment '员工id',
emp_name                  string  comment '员工姓名', 
create_date_min           string  comment '首次线索创建日期:yyyy-MM-dd',
create_date_max           string  comment '首次线索创建日期:yyyy-MM-dd',
distribute_date_min       string  comment '首次上户日期:yyyy-MM-dd',
distribute_date_max       string  comment '首次上户日期:yyyy-MM-dd',
first_see_date_min        string  comment '首次带看日期:yyyy-MM-dd',
first_see_date_max        string  comment '首次带看日期:yyyy-MM-dd',
first_subscribe_date_min  string  comment '首次认购日期:yyyy-MM-dd',
first_subscribe_date_max  string  comment '首次认购日期:yyyy-MM-dd',
first_sign_date_min       string  comment '首次签约日期:yyyy-MM-dd',
first_sign_date_max       string  comment '首次签约日期:yyyy-MM-dd',
clue_num                  bigint  comment '线索量',
distribute_num            bigint  comment '上户量',
see_num                   bigint  comment '带看量',
see_project_num           bigint  comment '带看楼盘量',
subscribe_num             bigint  comment '认购量:含退、含外联',
sign_num                  bigint  comment '签约量:含退、含外联',
call_duration             bigint  comment '线索通话时长(秒)',
call_num                  bigint  comment '线索通话次数',
clue_see_num              bigint  comment '产生带看的线索量',
clue_subscribe_num        bigint  comment '产生认购的线索量',
clue_sign_num             bigint  comment '产生签约的线索量',
etl_time                  string  comment 'ETL跑数时间' 
) comment 'global_id粒度线下业务指标表' 
stored as parquet 
;

