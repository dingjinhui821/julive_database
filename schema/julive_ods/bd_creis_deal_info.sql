drop table if exists ods.bd_creis_deal_info;
create external table ods.bd_creis_deal_info(
id                                                     bigint          comment 'id',
city_id                                                int             comment '城市',
district_id                                            int             comment '区域',
project_type                                           int             comment '业态',
from_page                                              int             comment '爬取页面板块 1.项目， 2.市场',
deal_num                                               int             comment '成交套数',
deal_acreage                                           double          comment '成交面积',
deal_price                                             double          comment '成交价格',
deal_amount                                            double          comment '成交金额',
market_num                                             int             comment '上市套数',
market_acreage                                         double          comment '上市面积',
saleable_num                                           int             comment '可售套数',
saleable_acreage                                       double          comment '可售面积',
project_type_all                                       string          comment '全部业态 多个逗号分隔',
start_datetime                                         int             comment '开始时间',
end_datetime                                           int             comment '结束时间',
min_time_type                                          int             comment '最小时间维度、 1.天， 2.周， 3.月',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_creis_deal_info'
;
