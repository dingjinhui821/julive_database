drop table if exists ods.yw_employee_project_case_sale;
create external table ods.yw_employee_project_case_sale(
id                                                     int             comment 'id',
project_id                                             bigint          comment '楼盘id',
sales_name                                             string          comment '销售姓名',
sales_mobile                                           string          comment '销售电话',
summary                                                string          comment '备注',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_project_case_sale'
;
