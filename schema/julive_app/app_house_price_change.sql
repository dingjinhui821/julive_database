drop table if exists julive_app.app_house_price_change;
create table julive_app.app_house_price_change(
 
house_id                  string COMMENT '户型id',
project_id                string COMMENT '楼盘id',  
final_price               string COMMENT '最终价格', 
last_but_one_price        string COMMENT '上一个价格',
final_down_pay            string COMMENT '最终首付',
last_but_one_down_pay     string COMMENT '最终首付',
final_create_date         string COMMENT '最终价格改动时间',
last_but_one_create_date  string COMMENT '上一个价格改动时间',
price_status              int    COMMENT '1-降价，2-涨价',
cut_price                 int    COMMENT '降价或者涨价金额'
)
stored as parquet;
