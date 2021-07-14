drop table if exists ods.kfs_project_clue_detail;
create external table ods.kfs_project_clue_detail(
id                                                     int             comment '主键id',
big_data_id                                            int             comment '大数据侧线索id',
order_time                                             int             comment '线索生成时间',
city_id                                                int             comment '城市id',
developer_id                                           int             comment '开发商id',
project_id                                             int             comment '楼盘id',
project_name                                           string          comment '楼盘名称',
saler_name                                             string          comment '销售姓名',
saler_phone                                            string          comment '销售电话',
channel_id                                             int             comment '渠道id',
user_id                                                string          comment '线索客户id',
user_phone                                             string          comment '线索客户电话',
district_ids                                           string          comment '感兴趣区域',
min_price                                              double          comment '感兴趣最小价格',
max_price                                              double          comment '感兴趣最大价格',
project_cost                                           double          comment '线索成本',
clue_price                                             double          comment '线索售价',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_project_clue_detail'
;
