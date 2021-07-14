drop table if exists ods.yw_quality_sample_order;
create external table ods.yw_quality_sample_order(
id                                                     int             comment 'id',
task_project_id                                        int             comment '作业id',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '咨询师id',
employee_name                                          string          comment '咨询师名字',
employee_manager_id                                    bigint          comment '咨询师主管id',
employee_manager_name                                  string          comment '咨询师主管名字',
distribute_datetime                                    int             comment '分配时间',
city_id                                                int             comment '城市id',
city_name                                              string          comment '城市名称',
call_type                                              int             comment '通话类型 通话类型 1主叫 2被叫',
call_duration                                          int             comment '通话时长',
call_times                                             int             comment '通话次数',
contact_times                                          int             comment '联系次数',
is_result                                              int             comment '是否检查（0:未查 1:已查）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
is_del                                                 int             comment '是否删除 0未删 1已删除',
quality_status                                         int             comment '质检是否合格1:合格2',
quality_person                                         int             comment '质检者',
quality_datetime                                       int             comment '质检时间',
quality_type                                           int             comment '质检类型1:普通质检2:a类问题3:无效样本4:中断录音',
alloc_num                                              int             comment '上户数',
distribute_employee_id                                 bigint          comment '所属咨询师咨询师id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_sample_order'
;
