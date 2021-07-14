set hive.execution.engine=spark;
set spark.app.name=fact_refund_pay_fee_detail_dtl;  
set spark.yarn.queue=etl;

INSERT overwrite TABLE julive_fact.fact_refund_pay_fee_detail_dtl
SELECT tmp1.pay_datetime,
       tmp1.pay_money,
       tmp1.creator,
       tmp1.employee_name,
       tmp1.city_id,
       tmp1.city_name,
       tmp1.bank_flow_number,
       tmp1.arrival_time,
       tmp1.arrival_money,
       tmp1.company_name,
       tmp1.payee_full_name,
       tmp1.accounts,
       tmp1.deal_id,
       tmp1.type
FROM
  (SELECT if(t2.pay_datetime = 0, NULL, from_unixtime(t2.pay_datetime)) AS pay_datetime,
          t2.pay_money,
          t1.creator,
          t5.employee_name,
          t2.city_id,
          t6.city_name,
          t4.bank_flow_number,
          from_unixtime(t4.arrival_time) AS arrival_time,
          t4.arrival_money,
          t1.company_name,
          t1.payee_full_name,
          t1.accounts,
          t3.deal_id,
          case 
          when t1.refund_type = 1 then '退成交单'
          when t1.refund_type = 2 then '退流水'
          else '其他'
          end          AS type
   FROM
    ods.cw_payment_approval t1 
   LEFT JOIN
     (SELECT *
      FROM ods.cw_accounting_fee_detail
      WHERE TYPE = 1
        AND is_delete = 2 ) t2 ON t1.approval_id = t2.link_id
   LEFT JOIN
     (SELECT *
      FROM ods.cw_deal_refund_to_bank_flow) t3 ON t1.approval_id = t3.approval_id
   LEFT JOIN ods.ex_bank_flow    t4 ON t3.bank_flow_id = t4.id
   LEFT JOIN ods.yw_employee     t5 ON t1.creator = t5.id
   LEFT JOIN julive_dim.dim_city t6 ON t2.city_id = t6.city_id
   WHERE t2.pay_datetime != 0 
   and t1.refund_type in(1,2)

   )tmp1 ;
