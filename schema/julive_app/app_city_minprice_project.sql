drop table if exists julive_app.app_city_minprice_project;
create table julive_app.app_city_minprice_project(
project_id              string COMMENT '楼盘ID',
project_name            string COMMENT '楼盘名称',
price_min               int    COMMENT '楼盘最低价', 
project_city            int    COMMENT '城市id，城市化专用的分隔标记', 
district_id             bigint COMMENT '区域id', 
district_name           string COMMENT '区域名称'    
)
stored as parquet;