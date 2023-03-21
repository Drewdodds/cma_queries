/*
Cost source value priority list
List price * multiplier if its been updated within a year
Last purchase price
Current Cost
*/


with latest_calcualted_cost as (select ITEM_NUMBER,
       trim(VENDOR_MULTIPLIER) as VENDOR_MULTIPLIER,
       VENDOR_LIST_PRICE,
       round((VENDOR_MULTIPLIER * VENDOR_LIST_PRICE),2) as calcualted_cost
from ANALYTICS.FWS_PRODUCT.VENDOR_INFO_COSTS
where LAST_MODIFIED_AT between dateadd('year', -1, current_date) and current_date
and VENDOR_MULTIPLIER is not null
and VENDOR_LIST_PRICE is not null
and VENDOR_MULTIPLIER not in ('25','CQ')
and VENDOR_STATUS = 'active'
and nullif(trim(VENDOR_MULTIPLIER),'') is not null),

latest_cost as (select ITEM_NUMBER,
       max(LAST_ORDER_DATE) as last_date,
       round(max(LAST_ORIGINATING_COST),2) as lastest_cost
from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS
group by 1
limit 100 )

select gp.ITEM_NUMBER,
       l1.calcualted_cost,
       l2.lastest_cost
       round(coalesce(calcualted_cost, lastest_cost, CURRENT_COST),2) as cost
from ANALYTICS.FWS_STAGING.GP_MASTER_ITEMS as gp
left join latest_calcualted_cost l1
    on l1.ITEM_NUMBER = gp.ITEM_NUMBER
left join latest_cost l2
    on l2.ITEM_NUMBER = gp.ITEM_NUMBER
