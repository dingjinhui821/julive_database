

-- DIM-扩展线索维度表

drop table if exists julive_dim.dim_clue_ext_base;
create table if not exists julive_dim.dim_clue_ext_base(
clue_id                      bigint       comment '线索ID',                                                                        
create_date                  string       comment '创建日期yyyy-MM-dd',                                                              
create_time                  string       comment '创建时间',                                                                        
channel_id                   int          comment '渠道id',                                                                        
org_id                       int          comment '公司id',                                                                        
org_type                     int          comment '类型 1 居理 2加盟', 
org_name                     string       comment '公司名称',    
from_source	                 int          comment '数据来源: 1-居理数据 2-乌鲁木齐虚拟项目数据 3-二手房中介项目数据 4-加盟商数据',
from_source_detail	         int          comment '数据来源: 1-居理数据 11-自营(ai客服) 2-乌鲁木齐虚拟项目数据 3-二手房中介项目数据 31-来源xpt_order二手房中介数据 4-加盟商数据',                                                                    
city_id                      int          comment '线索来源城市id',                                                                    
city_name                    string       comment '线索来源城市名称',                                                                    
source                       int          comment '订单了解途径',                                                                      
user_id                      bigint       comment '用户id',   
intent                       int          comment '1.无意向 2.保留 3.有意向', --
intent_tc                    string       comment '1.无意向 2.保留 3.有意向', --
is_short                     string       comment '是否缩短路径',  --
customer_intent_city_id      int          comment '客户意向城市ID',                                                                    
customer_intent_city_name    string       comment '客户意向城市名称',                                                                    
user_name                    string       comment '用户姓名',                                                                        
user_mobile                  string       comment '用户手机号',                                                                       
sex                          int          comment '用户性别1:男2:女',                                                                  
creator                      string       comment '创建人',                                                                         
emp_id                       int          comment '员工id',                                                                        
emp_name                     string       comment '员工姓名',                                                                        
is_400_called                string       comment '是否400来电,即客服订单',                                                               
is_distribute                int          comment '是否分配，及不分配的原因：1、分配2、谈合作3、超区域4、不关注楼盘5、必须找售楼处6、关注二手房7、之前已上户8、不愿留电话9、电话无法接通99、其他',
media_name                   string       comment '媒体类型名称',                                                                      
module_name                  string       comment '模块名称',                                                                        
device_name                  string       comment '设备名称',                                                                        
app_type_name                string       comment 'APP类型:苹果安卓',                                                                  
channel_city_name            string       comment '渠道城市',                                                                        
channel_name                 string       comment '渠道名称',                                                                        
secend_reason                string       comment '二级不上户原因',                                                                     
first_reason                 string       comment '一级不上户原因',                                                                     
app_source                   string       comment 'app归因渠道包',                                                                    
install_date                 string       comment '末次激活日期',                                                                      
install_time                 string       comment '末次激活时间', 
install_city_name            string       comment '末次激活城市名称',
install_app_type_name        string       comment '末次激活APP类型名称',                                                                   
aid                          string       comment 'app计划id',                                                                     
cid                          string       comment 'app创意id',                                                                     
full_type                    string       comment '所属咨询师是否转正',                                                                   
yw_line                      string       comment '业务线',                                                                         
order_tag                    string       comment 'abcs标签',                                                                      
etl_time                     string       comment 'ETL跑数日期'           
) comment 'DIM-扩展线索维度表' 
stored as parquet 
;
                                                           