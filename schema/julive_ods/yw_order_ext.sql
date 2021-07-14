drop table if exists ods.yw_order_ext;
create external table ods.yw_order_ext(
order_id                                               bigint          comment '订单id',
is_problem                                             int             comment '是否是问题单 1是 2否',
problem_reason                                         string          comment '问题原因',
is_solve                                               int             comment '是否解决 1是 2否',
solution                                               string          comment '解决方案说明',
is_visit                                               int             comment '是否到访 1已到访 2未到访',
from_city_id                                           int             comment '客户来源城市',
from_district_id                                       int             comment '客户来源区域id -1表示其他区域',
user_resistance_point                                  string          comment '客户抗性点',
user_cognitive_point                                   string          comment '客户认知点',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
other_district_name                                    string          comment '其他区域名称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_ext'
;
