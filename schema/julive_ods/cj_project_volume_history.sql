drop table if exists ods.cj_project_volume_history;
create external table ods.cj_project_volume_history(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
volume_id                                              int             comment '放量id',
volume_type                                            int             comment '放量类型 1已开盘、2已加推、3预计开盘、4预计加推',
volume_date_year                                       int             comment '放量时间年',
volume_date_month                                      int             comment '放量时间月',
volume_date_day                                        int             comment '放量时间日',
volume_date_ten                                        int             comment '放量时间旬 1上旬 2中旬 3下旬',
note                                                   string          comment '备注',
relation_json                                          string          comment '关联信息json',
is_send_change_notify                                  int             comment '是否发送变更通知 1是 2否',
is_need_paihao                                         int             comment '放量前是否要排号 1有排号 2没有排号',
paihao_status                                          int             comment '排号状态 1待排号 2正在排号',
paihao_start_datetime                                  int             comment '排号开始时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
status                                                 int             comment '状态 1正常 2删除',
relation_buildings                                     string          comment '关联楼栋id，逗号分隔',
relation_house_types                                   string          comment '关联户型id，逗号分隔',
volume_date                                            int             comment '逻辑转化放量时间，将年月日旬转化',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_volume_history'
;
