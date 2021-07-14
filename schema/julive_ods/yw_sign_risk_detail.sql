drop table if exists ods.yw_sign_risk_detail;
create external table ods.yw_sign_risk_detail(
id                                                     bigint          comment '',
sign_risk_id                                           bigint          comment '签约风险表id',
purchase_question_type                                 int             comment '客户问题类型:10客户购房资金是否有问题20:客户购房资质是否有问题30:客户是否有其他签约问题 40:客户/其他决策人是否对市场信心不足50:客户是否觉得项目产品力不合适（面积、户型、质量、容积率等），有动摇
60:客户是否暂时不方便签约（如不在国内等）70:政府限制签约名额导致签约排队80:开发商原因导致签约排队',
plan_money_place_datetime                              int             comment '预计资金到位时间',
customer_question_type                                 string          comment '10:卖房卖房,11:向金融机构借贷,12:个人资金周转（理财、基金等）,13:自己拆借（向家人、朋友、同事等借款）,14:贷款遇到问题需要解决,15:纳税社保、工作居住证等时间未到,16:受婚姻关系影响暂时拖延,17:无资质需中介办理,18:价格,19:面积,20:户型,21:房屋质量,22:容积率,23:配套,24:开发商,25:解除抵押,26:产权转让,27:开发商限签',
is_know                                                int             comment '是否有问题:1是2否3不知道',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sign_risk_detail'
;
