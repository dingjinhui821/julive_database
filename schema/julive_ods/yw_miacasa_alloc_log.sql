drop table if exists ods.yw_miacasa_alloc_log;
create external table ods.yw_miacasa_alloc_log(
id                                                     bigint          comment '主键',
order_id                                               bigint          comment '订单id',
city_id                                                bigint          comment '城市id',
employee_id                                            int             comment '咨询师id',
score                                                  int             comment '得分',
result                                                 string          comment '返回结果',
status                                                 int             comment '返回状态:0正常-1异常',
re_datetime                                            int             comment '接口响应时间',
create_datetime                                        int             comment '创建者',
update_datetime                                        int             comment '更新者',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_miacasa_alloc_log'
;
