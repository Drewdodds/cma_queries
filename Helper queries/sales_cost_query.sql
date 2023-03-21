with t1 as (select ITEM_NUMBER,
       VENDOR_ID,
       LAST_ORDER_DATE::date as LAST_ORDER_DATE,
       case when LAST_RECEIPT_COST = 0.00000 then LAST_ORIGINATING_COST else LAST_RECEIPT_COST end as cost,
       VENDOR_ITEM_DESCRIPTION,
       VENDOR_ITEM_NUMBER,
       dense_rank() over(partition by ITEM_NUMBER order by LAST_ORDER_DATE::date desc) as order_rank
from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS
where ITEM_NUMBER in ()
order by 1 asc, 3 desc ),

t2 as (select *,
       count(*) over(partition by ITEM_NUMBER) as num_of_dupes
from t1
where order_rank = 1
order by 1 asc ),

v_names as (select distinct trim(PRIMARY_VENDOR_ID) as primary_vendor_id,
       PRIMARY_VENDOR_NAME
from ANALYTICS.FWS_CORE.PRODUCTS ),

vendor_data as (select ITEM_NUMBER,
       VENDOR_ID,
       PRIMARY_VENDOR_NAME,
       VENDOR_ITEM_NUMBER,
       VENDOR_ITEM_DESCRIPTION,
       LAST_ORDER_DATE,
       cost
from t2
left join v_names
on v_names.primary_vendor_id = trim(t2.VENDOR_ID)
where num_of_dupes = 1),


sales as (select ITEM_NUMBER,
       round(sum(nullif(EXTENDED_PRICE,0)),2) as past_year_revenue,
       round(sum(EXTENDED_COST),2) as past_year_cost,
       round(sum(QUANTITY),1) as past_year_units_sold,
       past_year_revenue - past_year_cost as past_year_profit,
       past_year_profit / past_year_revenue as past_year_margin
from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
where SOP_TYPE = 'INVOICE'
and CANCELLATION_REASON is null
and RETURN_REASON is null
and DOCUMENT_DATE between dateadd('year', -1, current_date) and current_date
and CUSTOMER_CLASS <> 'PHARMACY'
and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
and ITEM_DESCRIPTION not ilike '%refund%'
and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
and ITEM_NUMBER not in ('F-NON-INV-MISNTX' , 'F-MISNTX', 'F-FRGT', 'Test Item')
and ITEM_NUMBER in ()
group by 1
order by 2 asc )


select v.*,
       s.*
from vendor_data v
left join sales s
on s.ITEM_NUMBER = v.ITEM_NUMBER
