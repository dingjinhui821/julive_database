drop table if exists ods.yw_owl_sop;
create external table ods.yw_owl_sop(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
district_id                                            string          comment '区域id',
see_project_id                                         bigint          comment '带看表id',
see_employee_id                                        bigint          comment '带看员工id',
project_id                                             string          comment '楼盘id',
see_project_datetime                                   int             comment '带看时间',
is_first_project                                       int             comment '首访楼盘，1是，2否',
panel_num                                              int             comment '区域面板展示次数',
traffic_num                                            int             comment '交通按钮展示次数',
support_num                                            int             comment '配套入口按钮展示次数',
page_view                                              double          comment '沙盘使用时长',
slide_map_num                                          int             comment '拖拽地图次数',
is_sop_ok                                              int             comment '操作是否合格，1是，2否',
didi_order_ids                                         string          comment '滴滴订单id',
gps_section                                            string          comment '带看打点区间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_owl_sop'
;
