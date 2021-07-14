drop table if exists ods.dsp_feed_area_stat;
create external table ods.dsp_feed_area_stat(
id                                                     int             comment '',
cur_date                                               int             comment '日期，存时间戳',
dsp_account_id                                         int             comment '账户id',
account_name                                           string          comment '账户名',
province_name                                          string          comment '省',
city_name                                              string          comment '城市',
download_finish                                        int             comment '下载完成数',
`convert`                                              int             comment '转化',
cost                                                   double          comment '消耗',
click                                                  int             comment '',
`show`                                                 int             comment '',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
ctr                                                    double          comment 'ctr',
cpc                                                    double          comment 'cpc',
cpm                                                    double          comment 'cpm',
media_type                                             int             comment '媒体类型（百度:1360:2搜狗:3今日头条:4）',
product_type                                           int             comment '产品形态（feed:1sem:4app:3）',
data_type                                              int             comment '数据类型 1.按区域统计，2.按账户统计',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_feed_area_stat'
;
