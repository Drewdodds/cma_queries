select PRODUCT_ID,
       ITEM_NUMBER,
       gp.VENDOR_ITEM_NUMBER as vendor_purchase_sku,
       VENDOR_ITEM_DESCRIPTION,
       gp.VENDOR_ID,
       RELEASED_DATE,
       PROMISED_DATE,
       UNIT_OF_MEASURE,
       UNIT_OF_MEASURE_QTY_IN_BASE,
              case when UNIT_OF_MEASURE_QTY_IN_BASE = 1 then UNIT_COST
            when UNIT_OF_MEASURE_QTY_IN_BASE > 1 then round((UNIT_COST / UNIT_OF_MEASURE_QTY_IN_BASE),2)
            end as fws_cost,
            UNIT_COST,
       row_number() over(partition by ITEM_NUMBER order by PROMISED_DATE desc) as promise_date
from ANALYTICS.FWS_STAGING.GP_PURCHASE_ORDER_LINES as gp
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s1
    on s1.GP_SKU = gp.ITEM_NUMBER
where gp.VENDOR_ITEM_NUMBER in()
or gp.ITEM_NUMBER in ()
qualify promise_date = 1