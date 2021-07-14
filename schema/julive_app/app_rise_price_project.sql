drop table if exists julive_app.app_rise_price_project;
create table julive_app.app_rise_price_project(
   
project_id              string COMMENT '楼盘ID',
project_name            string COMMENT '楼盘名称', 
project_city            int    COMMENT '城市id，城市化专用的分隔标记', 
final_price             int    COMMENT '最终总价',
cut_price               int    COMMENT '降价金额',
district_id             bigint COMMENT '区域id', 
district_name           string COMMENT '区域名称',    
final_create_date       string COMMENT '降价日期'
)
stored as parquet;