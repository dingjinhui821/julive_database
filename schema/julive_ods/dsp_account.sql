drop table if exists ods.dsp_account;
create external table ods.dsp_account(
id                                                     int             comment '',
media_type                                             int             comment '媒体类型（百度:1360:2搜狗:3今日头条:4）',
account                                                string          comment '账户',
params                                                 string          comment '请求api时需要传递的参数(json格式)',
param_id                                               int             comment '参数表id',
city_id                                                int             comment '城市id',
status                                                 int             comment '状态（1:启用2:停用）',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
product_type                                           int             comment '产品形态（feed:1sem:4app:3）',
product_type2                                          int             comment '投放类型（备用，包含app）',
app_type                                               int             comment 'app类型，0非app 1 ios 2 安卓',
agent                                                  string          comment '代理或者负责人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_account'
;
