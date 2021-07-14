drop table if exists ods.yw_project_partner;
create external table ods.yw_project_partner(
id                                                     bigint          comment '楼盘合作方id',
partner_name                                           string          comment '合作方名称',
before_notify_second                                   int             comment '提前多久报备，单位秒',
template                                               string          comment '报备模板',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新者',
creator                                                bigint          comment '创建者',
report_desc                                            string          comment '报备说明',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
type                                                   int             comment '公司属性 1平台公司 2城市开发商 3代理公司 4渠道公司 5物业公司',
status                                                 int             comment '状态 1正常 2删除',
key_person_id                                          int             comment '关键人id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_partner'
;
