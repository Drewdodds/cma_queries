with rev as (
                select ITEM_NUMBER,
                   round(sum(nullif(EXTENDED_PRICE,0)),2) as past_year_revenue,
                   round(sum(QUANTITY),1) as past_year_units_sold
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
            order by 2 desc
),

web_stock as (
                select GP_SKU,
                       INVENTORY_QUANTITY as available_inventory
                from ANALYTICS.FWS_STAGING.SHOPIFY_PRODUCT_VARIANT
                where SOURCE_RELATION = 'shopify_fws'
                and TITLE not ilike '%Auto renew'

    )
select s.PARENT_NAME as product_name,
       s.GP_SKU  as item_number,
       s.MANUFACTURER as brand,
       s.WEB_SUPER_CATEGORY,
       s.WEB_CATEGORY,
       s.WEB_SUB_CATEGORY,
       CREATED_AT::DATE as created_at,
       iff(NLADISPLAY_ID is not null, 'Discontinued', null) as discontinued_status,
       past_year_revenue,
       past_year_units_sold,
       ws.available_inventory,
       WEB_PRICE,
       ACTIVE_SALES_PROMO,
       PROMO_TYPE,
       PROMO_START_DATE,
       PROMO_END_DATE,
       PROMO_PRICE,
       PROMO_DISCOUNT_PERCENTAGE
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s
left join rev r
 on r.ITEM_NUMBER = s.GP_SKU
left join web_stock as ws
    on ws.GP_SKU = s.GP_SKU
where QUANTITY = 1
order by ITEM_NUMBER