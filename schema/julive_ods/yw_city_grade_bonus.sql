drop table if exists ods.yw_city_grade_bonus;
create external table ods.yw_city_grade_bonus(
id                                                     bigint          comment '',
city_id                                                int             comment '城市id',
city_status                                            int             comment '新旧城市，1旧城，2新城',
city_name                                              string          comment '城市名',
y_value                                                double          comment '城市y值',
city_employee_num                                      double          comment '城市人力数',
sign_contract_commission                               double          comment '签约佣金',
subscribe_contract_commission                          double          comment '认购佣金',
signgrass_contract_commission                          double          comment '草签且回佣金',
adjust_datetime                                        int             comment '核算时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
actual_sign_contract_commission                        double          comment '签约佣金差额',
actual_subscribe_contract_commission                   double          comment '认购佣金差额',
actual_signgrass_contract_commission                   double          comment '草签且回佣金差额',
calculation_log                                        string          comment '计算日志',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_city_grade_bonus'
;
