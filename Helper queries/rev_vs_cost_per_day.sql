with pos as (select  RELEASED_DATE::Date as date,
       PURCHASE_ORDER_ID,
       sum(EXTENDED_COST) as cogs_per_po
from ANALYTICS.FWS_STAGING.GP_PURCHASE_ORDER_LINES as pos
where RELEASED_DATE between dateadd('year', -1, current_date) and current_date
group by 1,2
order by 1 desc ),

agg_pos_by_day as (
    select date,
           sum(cogs_per_po) as cogs_per_day
               from pos
    group by 1
),


sales as (select DOCUMENT_DATE::date as date,
       sum(EXTENDED_PRICE) as rev_per_day
from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
where SOP_TYPE = 'INVOICE'
and CANCELLATION_REASON is null
and RETURN_REASON is null
and DOCUMENT_DATE between dateadd('year', -1, current_date) and current_date
and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
and ITEM_DESCRIPTION not ilike '%refund%'
and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
and ITEM_NUMBER not in ('F-NON-INV-MISNTX', 'F-MISNTX', 'F-FRGT', 'Test Item')
group by 1
order by 2 desc )

select agg_pos_by_day.date,
       cogs_per_day,
       rev_per_day,
       round((rev_per_day-cogs_per_day),2) as diff,
       sum(cogs_per_day) over() as past_year_cogs,
       sum(rev_per_day) over() as past_year_rev
from agg_pos_by_day join sales on sales.date = agg_pos_by_day.date
order by 1 desc