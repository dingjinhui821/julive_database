drop table if exists ods.common_dindin_send_record;
create external table ods.common_dindin_send_record(
id                                                     int             comment '',
type                                                   int             comment '类型,1:未上户的留电',
order_id                                               bigint          comment '订单id',
dindin_ids                                             string          comment '钉钉ids',
status                                                 int             comment '状态（1:启用2:停用）',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '编辑者',
leave_phone_time                                       int             comment '留电时间',
create_datetime                                        int             comment '创建时间，留电时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/common_dindin_send_record'
;
