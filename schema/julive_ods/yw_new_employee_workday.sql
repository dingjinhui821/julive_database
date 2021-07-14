drop table if exists ods.yw_new_employee_workday;
create external table ods.yw_new_employee_workday(
id                                                     int             comment '主键',
employee_id                                            int             comment '员工id',
manager_name                                           string          comment '主管姓名',
manager_id                                             int             comment '主管id',
begin_datetime                                         int             comment '开始时间',
end_datetime                                           int             comment '结束时间',
year                                                   int             comment '年',
week                                                   int             comment '周',
city_id                                                bigint          comment '城市id(业务核算城市)',
monday                                                 int             comment '星期一:1工作 2休息 3工作+值班 4休息+值班',
tuesday                                                int             comment '星期二:1工作 2休息 3工作+值班 4休息+值班',
wednesday                                              int             comment '星期三:1工作 2休息 3工作+值班 4休息+值班',
thursday                                               int             comment '星期四:1工作 2休息 3工作+值班 4休息+值班',
friday                                                 int             comment '星期五:1工作 2休息 3工作+值班 4休息+值班',
saturday                                               int             comment '星期六:1工作 2休息 3工作+值班 4休息+值班',
sunday                                                 int             comment '星期日:1工作 2休息 3工作+值班 4休息+值班',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
org_id                                                 int             comment '公司id',
is_change                                              int             comment '是否编辑过  1 是 2 否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_new_employee_workday'
;
