drop table if exists ods.dsp_account_structure_qihoo;
create external table ods.dsp_account_structure_qihoo(
id                                                     int             comment 'id',
media_type                                             int             comment '媒体类型（百度:1360:2搜狗:3今日头条:4）',
product_type                                           int             comment '产品形态（feed:1sem:4app:3）',
account                                                string          comment '账户',
account_id                                             int             comment '账户在dsp_account表里对应的id',
plan_id                                                bigint          comment '计划id',
plan_name                                              string          comment '计划名称',
unit_id                                                bigint          comment '单元id',
unit_name                                              string          comment '单元名称',
word_id                                                bigint          comment '关键词或者创意id，如果是关键词，是关键词id，如果是创意，这是创意id',
word                                                   string          comment '创意或者关键词',
word_status                                            int             comment '本条结构的状态 计划 21有效 22处于暂停阶段 23暂停推广 24推广计划预算不足 25账户预算不足 单元 31有效 32暂停推广 33推广计划暂停推广 关键词 40有效移动url审核中 41有效 42暂停推广 43不宜推广 44搜索无效 45待激活 46审核中 7搜索量过低 48部分无效 49计算机搜索无效 50移动搜索无效 创意51有效 52暂停推广 53不宜推广 54待激活 55审核中 56部分无效 57有效移动url审核中',
structure_type                                         int             comment '对象类型 2计划 3单元 4关键词 5创意',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_account_structure_qihoo'
;
