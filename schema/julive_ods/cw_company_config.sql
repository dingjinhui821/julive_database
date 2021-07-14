drop table if exists ods.cw_company_config;
create external table ods.cw_company_config(
id                                                     bigint          comment '',
company_name                                           string          comment '公司主体名称',
company_code                                           string          comment '公司主体code',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_company_config'
;
