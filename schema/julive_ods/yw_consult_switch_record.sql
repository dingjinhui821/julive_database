drop table if exists ods.yw_consult_switch_record;
create external table ods.yw_consult_switch_record(
id                                                     int             comment '',
datetime                                               int             comment '日期',
employee_id                                            int             comment '',
consult_start_datetime                                 int             comment '开/关上户开始时间 type:1 开上户时间  type:2 关上户时间',
consult_end_datetime                                   int             comment '开/关上户结束时间type:2 关上户时间  type:1 开上户时间',
cause_type                                             int             comment '原因类型:1.下班/休息关上户、2.外出带看、3.外出市调、4.集团或部门会议、5.新上户过多、6.库存客户过多、7.其他',
cause                                                  string          comment '经理关上户原因',
append_text                                            string          comment '追加内容',
append_text_id                                         int             comment '追加内容楼盘id 员工对应的id',
close_type                                             int             comment '关上户类型 1 咨询师,2 咨询师主管,3系统排班，4系统带看',
open_type                                              int             comment '开上户1 咨询师2 咨询师主管,3系统排班',
operator                                               string          comment '操作者',
type                                                   int             comment '开关上户类型 1开2关',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                int             comment '',
updator                                                int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_consult_switch_record'
;
