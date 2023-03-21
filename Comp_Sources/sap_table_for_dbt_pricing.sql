with updated_sap_records as (select *,
       row_number() over(partition by sku,TOTAL_LOWEST_PRICE_MERCHANT order by modified desc) as most_recent_comp_record_per_merch
from ANALYTICS.FWS_STAGING.STRIKE_A_PRICE_PRICES as sap
where TOTAL_LOWEST_PRICE_MERCHANT <> 'Fresh Water Systems'
and TOTAL_LOWEST_PRICE_MERCHANT not ilike '%ebay%'
qualify most_recent_comp_record_per_merch = 1
order by sku, modified desc )

select *,
       row_number() over(partition by SKU order by TOTAL_LOWEST_PRICE asc) as comp_rank
from updated_sap_records
qualify comp_rank = 1
order by SKU asc
