drop table if exists ods.xpt_report;
create external table ods.xpt_report(
id                                                     int             comment '主键自增id',
order_id                                               int             comment '订单id',
cj_user_id                                             int             comment 'cj_user用户id',
store_user_id                                          int             comment '经纪人id',
project_manager_id                                     int             comment '项目经理id',
username                                               string          comment '用户姓名',
mobile                                                 string          comment '手机号',
mobile_backup                                          string          comment '备用手机号',
project_id                                             int             comment '楼盘id',
expect_see_time                                        int             comment '期望带看时间',
status                                                 int             comment '报备状态:0未处理 1有效 2无效',
invalid_reason                                         string          comment '报备无效原因',
store_id                                               int             comment '门店id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_report'
;
