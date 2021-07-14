drop table if exists julive_dim.dim_dsp_account_rebate;
CREATE EXTERNAL TABLE julive_dim.dim_dsp_account_rebate(
id                                                  int,
dsp_account_id                                      int           COMMENT 'dsp账户id', 
rebate_timestamp                                    int           COMMENT '返点日期，存储时间戳', 
rebate_date                                         string        COMMENT '返点日期，yyyy-MM-dd',
start_date                                          string        COMMENT '返点开始执行日期，yyyy-MM-dd',
end_date                                            string        COMMENT '返点结束执行日期，yyyy-MM-dd',
rebate                                              double	      COMMENT '返点',
is_repair_history                                   int           COMMENT '是否修复历史数据，1:已修复或者不需要修复 2 未修复  当管理员修改返点后，会重新计算消费信息',
etl_time                                            string        comment 'ETL跑数时间 yyyy-MM-dd HH:mm:ss'
)
COMMENT '渠道返点维度表'
stored as parquet;

