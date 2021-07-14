drop table if exists ods.cj_developer_order;
create external table ods.cj_developer_order(
id                                                     int             comment 'id',
city_id                                                int             comment '城市id',
developer_id                                           int             comment '开发商id',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
saler_name                                             string          comment '销售名称',
saler_phone                                            string          comment '销售电话',
channel_id                                             int             comment '渠道id',
user_phone                                             string          comment '客户联系方式',
creator                                                int             comment '创建者id',
updator                                                int             comment '更新者id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
op_type                                                int             comment '留电optype',
user_id                                                string          comment '用户id',
district_ids                                           string          comment '区域',
min_price                                              int             comment '(网站)用户填写的最低总价',
max_price                                              int             comment '(网站)用户填写的最高总价',
kfs_order_id                                           int             comment '新表-kfs_order开发商订单id',
kfs_user_id                                            bigint          comment '新表-kfs_order用户id',
sync_flag                                              int             comment '当前表数据同步到新表-kfs_order标识:1-同步成功2-未同步10-同步失败-未知错误 11-同步失败-开发商为空 12-同步失败-创建订单时抛出异常  13-同步失败-用户为空  30-同步失败-楼盘为空  40-同步失败-用户手机号为空  50-同步失败-重复订单',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_developer_order'
;
