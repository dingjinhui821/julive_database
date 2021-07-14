drop table if exists ods.yw_owl_monitor;
create external table ods.yw_owl_monitor(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
see_project_id                                         bigint          comment '带看id',
see_employee_id                                        bigint          comment '带看咨询师id',
city_id                                                bigint          comment '城市id',
city_name                                              string          comment '城市名称',
from_stime                                             int             comment '去程开始时间',
from_etime                                             int             comment '去程结束时间',
to_stime                                               int             comment '回程开始时间',
to_etime                                               int             comment '回程结束时间',
see_stime                                              int             comment '总带看开始时间',
see_etime                                              int             comment '总带看结束时间',
project_stime                                          int             comment '楼盘内开始时间',
project_etime                                          int             comment '楼盘内结束时间',
replay_stime                                           int             comment '复盘开始时间',
replay_etime                                           int             comment '复盘结束时间',
project_ids                                            string          comment '带看楼盘id',
didi_order_ids                                         string          comment '打车id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_owl_monitor'
;
