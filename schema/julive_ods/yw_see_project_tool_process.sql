drop table if exists ods.yw_see_project_tool_process;
create external table ods.yw_see_project_tool_process(
id                                                     int             comment '主键id',
see_project_id                                         int             comment '带看id',
room_id                                                int             comment '房间id',
materials_type                                         int             comment '（1.个人 2.流程3.公司4.市场5.需求6.区域7.楼盘项目8.总结）',
object_id                                              int             comment '当前项目id（区域 或楼盘）',
status                                                 int             comment '1.未开始 2.进行中 3.已结束',
start_time                                             int             comment '开始时间',
end_time                                               int             comment '结束时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建着',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_see_project_tool_process'
;
