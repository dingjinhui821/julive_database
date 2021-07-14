drop table if exists ods.yw_see_project_list;
create external table ods.yw_see_project_list(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
see_project_id                                         bigint          comment '带看表id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
note                                                   string          comment '备注',
report_status                                          int             comment '报备状态  0:无需报备  1: 未报备 2: 已申请报备 3:报备完成  4:有效客户  5:无效客户',
project_grade                                          int             comment '楼盘评价等级，0分，1分，2分，3分，4分，5分',
project_comment                                        string          comment '楼盘评论',
report_datetime                                        int             comment '报备时间',
sale_realname                                          string          comment '售楼处销售姓名',
sale_mobile                                            string          comment '售楼处销售手机',
quality_rate                                           int             comment '品质评星',
area_rate                                              int             comment '区域评星',
cost_rate                                              int             comment '性价比评星',
useless                                                int             comment '评价无用',
useful                                                 int             comment '评价有用',
free_card                                              int             comment '免费排卡 0默认不免费排卡 1免费排卡',
cooperate                                              int             comment '是否是合作楼盘，1:是  2:否',
see_employee_id                                        bigint          comment '带看人的员工id',
sales_name                                             string          comment '销售姓名',
sales_phone                                            string          comment '销售电话',
sort                                                   int             comment '排序',
add_type                                               int             comment '添加类型，1计划带看添加，2实际带看添加',
special_car                                            int             comment '是否有专车',
didi_employee_id                                       int             comment '打车人员工id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_see_project_list'
;
