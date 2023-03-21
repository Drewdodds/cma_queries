with salsify_list as (select GP_SKU
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where GP_SKU in ()
and QUANTITY = 1
and NLADISPLAY_ID is null
and IS_WEB_VARIANT_PUBLISHED = 1),

updated_sap_records as (select *,
       row_number() over(partition by sku,TOTAL_LOWEST_PRICE_MERCHANT order by modified desc) as most_recent_comp_record_per_merch
from ANALYTICS.FWS_STAGING.STRIKE_A_PRICE_PRICES as sap
where MY_PRICE is not null
and MY_PRICE not ilike '%na%'
and TOTAL_LOWEST_PRICE_MERCHANT <> 'Fresh Water Systems'
and TOTAL_LOWEST_PRICE_MERCHANT not ilike '%ebay%'
and sku in (select gp_sku from salsify_list)
qualify most_recent_comp_record_per_merch = 1
order by sku, modified desc )

select ID,
       sku,
       gtin,
       TOTAL_LOWEST_PRICE,
       TOTAL_LOWEST_PRICE_MERCHANT,
       LOWEST_SHIPPING,
       row_number() over(partition by SKU order by TOTAL_LOWEST_PRICE asc) as comp_rank
from updated_sap_records
qualify comp_rank = 1
order by sku asc


--test