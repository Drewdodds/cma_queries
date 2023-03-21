select ITEM_NUMBER,
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
and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
and ITEM_DESCRIPTION not ilike '%refund%'
and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
and ITEM_NUMBER not in ('F-NON-INV-MISNTX' , 'F-MISNTX', 'F-FRGT', 'Test Item')
and ITEM_NUMBER in ()
group by 1
order by 2 desc