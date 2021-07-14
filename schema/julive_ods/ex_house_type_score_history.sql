drop table if exists ods.ex_house_type_score_history;
create external table ods.ex_house_type_score_history(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
score_id                                               bigint          comment '得分id',
house_type_id                                          int             comment '户型id',
acreage                                                double          comment '建筑面积',
room_type                                              int             comment '户型居室',
price                                                  double          comment '总价',
total_score                                            double          comment '户型得分',
on_sale_num                                            int             comment '在售房源余量',
status                                                 int             comment '户型销售状态，1: 未售  2:在售  3:售罄 4:待售 5: 排卡中',
on_sale_num_score                                      double          comment '在售余量得分',
sale_status                                            int             comment '户型销售状态',
volume_date                                            int             comment '放量时间',
is_need_paihao                                         int             comment '放量前是否要排号 1有排号 2没有排号',
paihao_start_datetime                                  int             comment '排号开始时间',
volume_num                                             int             comment '放量数量',
set_commission                                         double          comment '户型套佣',
set_commission_score                                   double          comment '户型套佣得分',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
contract_id                                            int             comment '合同id',
category_id                                            int             comment '合同分类id',
version_num                                            bigint          comment '版本号',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_house_type_score_history'
;
