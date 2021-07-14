drop table if exists ods.bd_spider_config_detail;
create external table ods.bd_spider_config_detail(
id                                                     bigint          comment '主键id',
type                                                   int             comment '配置类型:1新房，2二手房',
source                                                 int             comment '抓取源 1.搜房， 2.安居客， 3.搜狐焦点，5.贝壳',
city_id                                                int             comment '城市id',
field_type                                             int             comment '字段类型:1南方城市、2楼盘城市拼音映射关系、3爬去链接、4区域、5楼盘环线、6楼盘便签、7户型标签、8业态、9是否抓取板块、10品牌开发商映射关系',
config_info                                            string          comment '配置详细信息',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人id',
updator                                                int             comment '更新人id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_spider_config_detail'
;
