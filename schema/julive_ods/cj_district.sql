drop table if exists ods.cj_district;
create external table ods.cj_district(
id                                                     bigint          comment '',
parent_id                                              bigint          comment '父级id',
type                                                   int             comment '1:省  2:市  3:区县  4:乡镇  5:商圈',
name_cn                                                string          comment '中文名字',
name_pinyin                                            string          comment '拼音名字',
status                                                 int             comment '1: 有效  2:暂停',
show_index                                             int             comment '排序',
coordinate                                             string          comment '坐标',
abbreviation                                           string          comment '城市郁闷缩写',
baidu_city_code                                        int             comment '百度map对应的city_code',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
server_order_index                                     int             comment '客服添加订单区域排序',
is_start_close_shanghu                                 int             comment '是否该城市带看开始半小时前自动关闭上户:0启动1不启动',
alias                                                  string          comment '区域别名， “,” 分隔',
sign_average                                           int             comment '网签周期',
map_zoom                                               double          comment '地图缩放值',
baidu_adcode                                           int             comment '行政区县代码',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_district'
;
