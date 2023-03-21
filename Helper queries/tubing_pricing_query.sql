with reference_skus as (select PARENT_ID,
       PRODUCT_ID,
       GP_SKU,
       QUANTITY,
       PRODUCT_NAME,
       trim(split_part(split_part(PRODUCT_NAME, '-', 2),'f',1)) as feet,
       PRICE_REFERENCE_SKU,
       PRICE_REFERENCE_RULE,
       PRICE_REFERENCE_PERCENTAGE,
       WEB_PRICE,
       round(((WEB_PRICE-VAR_COST)/WEB_PRICE),2) as gm
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where WEB_CATEGORY = 'Polyethylene LLDPE Tubing'
and MANUFACTURER = 'Neo-Pure'
and PRODUCT_NAME ilike '%roll%'
and PRICE_REFERENCE_SKU is not null
order by gm asc ),

jg_rolls as (select PRODUCT_ID,
       PRODUCT_NAME,
       trim(split_part(split_part(PRODUCT_NAME, '-', 2),'f',1)) as feet,
       WEB_PRICE_MSRP,
       WEB_PRICE
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where PRODUCT_ID in (select reference_skus.PRICE_REFERENCE_SKU from reference_skus)
    ),

jg_price_per_ft as (select *,
       round((WEB_PRICE/feet),2) as price_per_ft_jg
from jg_rolls )

select r.*,
       round((r.web_price/r.feet),2) as price_per_ft_np,
       price_per_ft_jg,
       1 - (price_per_ft_np /price_per_ft_jg) as pct_diff,
       round((price_per_ft_jg * .9),2) as new_np_price_by_ft,
       round((new_np_price_by_ft * r.feet),2) as new_roll_price
from reference_skus r
join jg_price_per_ft j
    on j.PRODUCT_ID = r.PRICE_REFERENCE_SKU