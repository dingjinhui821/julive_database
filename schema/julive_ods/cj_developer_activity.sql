drop table if exists ods.cj_developer_activity;
create external table ods.cj_developer_activity(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id',
project_id                                             int             comment '楼盘id',
city_id                                                int             comment '城市',
activity_desc                                          string          comment '优惠信息',
discount_amount                                        int             comment '优惠金额',
activity_type                                          int             comment '活动类别优惠标识',
is_valid                                               int             comment '状态 1:有效，2:无效',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
basic_score                                            int             comment '活动推荐基础分,默认 100 （范围0～1000）',
activity_category                                      int             comment '活动类别（大类）1，2.22挑房节2，2.22返场狂欢，默认2',
project_name                                           string          comment '楼盘名称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_developer_activity'
;
