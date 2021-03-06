drop table if exists ods.ex_contract_category;
create external table ods.ex_contract_category(
id                                                     int             comment '合同分类id',
contract_id                                            int             comment '合同id',
business_layout                                        string          comment '楼栋类型id，逗号分隔',
building_number                                        string          comment '楼栋号码id，逗号分隔',
building_type                                          string          comment '户型类型id，逗号分隔',
room_hall                                              string          comment '几室几厅，逗号分隔，室厅以-连接',
acreage_type                                           int             comment '面积类型 1建筑面积 2室内面积',
acreage_min                                            double          comment '最小面积',
acreage_max                                            double          comment '最大面积',
commission_type                                        string          comment '佣金类型 1前置电商 2后置返费 3成交奖',
ecom_money                                             double          comment '电商金额',
ecom_pay_type                                          int             comment '电商结佣类型 1一次性结 2分阶段结',
ecom_pay_step                                          string          comment '电商结佣阶段 5排号 10认购 15草签 20网签',
paihao_money                                           double          comment '排号刷电商金额',
subscribe_money                                        double          comment '认购刷电商金额',
grass_money                                            double          comment '草签刷电商金额',
sign_money                                             double          comment '网签刷电商金额',
back_type                                              string          comment '返费金额算法 1按成交套数 2按成交额点位结佣，多个用逗号分隔',
back_money                                             double          comment '返费金额（元）',
is_paid                                                int             comment '是否有垫佣 1有',
paid_percent                                           double          comment '垫佣打折比',
back_money_type                                        int             comment '返费结算方式 1单笔结佣 2按批次结佣',
back_money_time_type                                   int             comment '返费结算方式时间类型 1次月 2次次月 3次周 4次次周',
back_money_detail_time                                 int             comment '返费结算方式具体时间',
back_money_account_day                                 int             comment '返费到帐天数(已废弃)',
back_money_apply_type                                  int             comment '返费申请分阶段情况 1一次性申请结 2分阶段申请结 3按客户交钱比例进行返费',
back_money_apply_step                                  string          comment '返费申请阶段  25草签 30网签 35网签且首付款交齐 40首付且提交完贷款资料 45网签且贷款审批通过 50网签且全款到账',
full_pay_times                                         int             comment '全款分期次数',
subscribe_back_percent                                 double          comment '认购返费金额(已废弃)',
grass_back_percent                                     double          comment '草签返费百分比(已废弃)',
sign_back_percent                                      double          comment '网签返费百分比(已废弃)',
sign_first_back_percent                                double          comment '网签且首付款交齐返费百分比(已废弃)',
first_load_back_percent                                double          comment '网签且提交完贷款资料返费百分比(已废弃)',
sign_load_back_percent                                 double          comment '网签且贷款审批通过返费百分比(已废弃)',
sign_full_back_percent                                 double          comment '网签且全款到账返费百分比(已废弃)',
first_pay_back_percent                                 double          comment '全款分期第1次交钱后返费百分比(已废弃)',
second_pay_back_percent                                double          comment '全款分期第2次交钱后返费百分比(已废弃)',
third_pay_back_percent                                 double          comment '全款分期第3次交钱后返费百分比(已废弃)',
four_pay_back_percent                                  double          comment '全款分期第4次交钱后返费百分比(已废弃)',
status                                                 int             comment '状态 1正常 2删除(已废弃)',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
area_error_range                                       double          comment '面积误差范围',
payment_type                                           string          comment '客户付款方式,1贷款 2全款一次性付清 3全款分期付清',
subscribe_sign_min_days                                int             comment '认购到签约时长最小天数',
subscribe_sign_max_days                                int             comment '认购到签约时长最大天数',
subscribe_first_min_days                               int             comment '认购到首付款交齐时长最小天数',
subscribe_first_max_days                               int             comment '认购到首付款交齐时长最大天数',
first_pay_min_percent                                  double          comment '首付比例最小值',
first_pay_max_percent                                  double          comment '首付比例最大值',
back_percent                                           double          comment '返费百分比',
total_price_type                                       int             comment '房屋总价类型 1房屋总价 2原价 3原价+溢价 4毛坯价 5毛坯价+装修价',
expected_total_price                                   double          comment '预计此分类房屋总价',
rate                                                   double          comment '费率',
price_type                                             int             comment '价格类型 1房屋总价 2原价 3原价+溢价 4毛坯价 5毛坯价+装修价',
price_min                                              double          comment '价格最大值',
price_max                                              double          comment '价格最大值',
price_error_range                                      double          comment '价格误差范围',
decorate_info                                          int             comment '装修情况，1.有装修，2毛坯',
reward_back_type                                       string          comment '成交奖返费金额算法 1按成交套数 2按成交额点位结佣，多个用逗号分隔',
reward_back_money                                      double          comment '成交奖返费金额（元）',
reward_is_paid                                         int             comment '成交奖是否有垫佣 1有',
reward_paid_percent                                    double          comment '成交奖垫佣打折比',
reward_back_money_type                                 int             comment '成交奖返费结算方式 1单笔结佣 2按批次结佣',
reward_back_money_time_type                            int             comment '成交奖返费结算方式时间类型 1次月 2次次月 3次周 4次次周',
reward_back_money_detail_time                          int             comment '成交奖返费结算方式具体时间',
reward_back_money_apply_type                           int             comment '返费申请分阶段情况 1一次性申请结 2分阶段申请结 3按客户交钱比例进行返费',
reward_back_money_apply_step                           string          comment '成交奖返费申请阶段 25草签 30网签 35网签且首付款交齐 40网签且提交完贷款资料 45网签且贷款审批通过 50网签且全款到账',
reward_full_pay_times                                  int             comment '成交奖全款分期次数',
reward_back_percent                                    double          comment '成交奖返费百分比',
reward_total_price_type                                int             comment '成交奖房屋总价类型 1房屋总价 2原价 3原价+溢价 4毛坯价 5毛坯价+装修价',
common_price_type                                      int             comment '价格类型（用于成交单页面初始化）1.房屋总价2原价3原价+溢价4毛坯价5毛坯价+装修价',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_contract_category'
;
