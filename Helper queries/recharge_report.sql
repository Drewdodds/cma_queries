with raw_list as (select CUSTOMER_ID as recharge_customer_id,
       SHOPIFY_CUSTOMER_ID,
       FIRST_NAME,
       LAST_NAME,
       EMAIL,
       PRODUCT_TITLE,
       r1.SHOPIFY_PRODUCT_ID,
       r1.SHOPIFY_VARIANT_ID,
       sku,
       s.PRODUCT_ID as salsify_product_id,
       PRICE as current_subcription_price,
       ORDER_INTERVAL_UNIT,
       ORDER_INTERVAL_FREQUENCY,
       NEXT_CHARGE_SCHEDULED_AT,
       NUMBER_SUBSCRIPTIONS,
       NUMBER_ACTIVE_SUBSCRIPTIONS
from ANALYTICS.FWS_STAGING.RECHARGE_SUBSCRIPTIONS as r1
left join ANALYTICS.FWS_STAGING.RECHARGE_CUSTOMER as r2
    on r1.CUSTOMER_ID = r2.ID
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s
    on r1.SKU = s.GP_SKU
where r1.STATUS = 'ACTIVE'
order by 1 asc ),

pipe_skus as (select recharge_customer_id,
                     sku,
       r.SHOPIFY_PRODUCT_ID,
       PRODUCT_ID as salsify_product_id,
       s2.GP_SKU as replacement_sku
from raw_list as r
join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s2
    on r.SHOPIFY_PRODUCT_ID::string = s2.SHOPIFY_PRODUCT_ID::string
where salsify_product_id is null
and SKU ilike '%|%' ),

deleted_pack_vars as (select recharge_customer_id,
                             sku,
       r2.SHOPIFY_PRODUCT_ID,
       split_part(sku, ':', 1) as base_sku,
       replace(split_part(sku, ':', 2),'PK','')::int as qty_in_pack,
       'Yes' as deleted_pack_sku,
       PRODUCT_ID as salsify_product_id,
       GP_SKU as replacement_sku
from raw_list as r2
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s3
    on s3.SHOPIFY_PRODUCT_ID::string = r2.SHOPIFY_PRODUCT_ID::string
where salsify_product_id is null
and SKU ilike '%:%'
and s3.QUANTITY =1 ),

changed_skus as (select recharge_customer_id,
       SKU,
       r3.SHOPIFY_PRODUCT_ID,
       s4.PRODUCT_ID as salsify_product_id,
       s4.GP_SKU as replacement_sku
from raw_list r3
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s4
    on s4.SHOPIFY_PRODUCT_ID::string = r3.SHOPIFY_PRODUCT_ID::string
where SKU not ilike '%|%'
and SKU not ilike '%:%'
and salsify_product_id is null
and s4.QUANTITY = 1 ),

final as (select r5.recharge_customer_id,
       SHOPIFY_CUSTOMER_ID,
       FIRST_NAME,
       LAST_NAME,
       EMAIL,
       PRODUCT_TITLE,
       r5.SHOPIFY_PRODUCT_ID,
       r5.SKU as recharge_sku,
       coalesce(p.replacement_sku, d.replacement_sku, c.replacement_sku) as repalcement_sku,
       deleted_pack_sku,
       base_sku,
       qty_in_pack,
       coalesce(r5.salsify_product_id, p.salsify_product_id, d.salsify_product_id, c.salsify_product_id) as salsify_product_id,
       current_subcription_price,
       case when qty_in_pack is not null then round((WEB_PRICE*qty_in_pack))
            else WEB_PRICE
            end as current_web_price,
       round((s5.VAR_COST),2) as current_cost,
       round(((current_subcription_price - current_cost)/current_subcription_price),2) as current_gross_margin,
       ORDER_INTERVAL_FREQUENCY,
       ORDER_INTERVAL_UNIT,
       NEXT_CHARGE_SCHEDULED_AT,
       NUMBER_ACTIVE_SUBSCRIPTIONS
from raw_list r5
left join pipe_skus as p
    on r5.recharge_customer_id = p.recharge_customer_id and r5.SHOPIFY_PRODUCT_ID = p.SHOPIFY_PRODUCT_ID
left join deleted_pack_vars as d
    on r5.recharge_customer_id = d.recharge_customer_id and r5.SHOPIFY_PRODUCT_ID = d.SHOPIFY_PRODUCT_ID
left join changed_skus as c
    on r5.recharge_customer_id = c.recharge_customer_id and r5.SHOPIFY_PRODUCT_ID = c.SHOPIFY_PRODUCT_ID
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s5
    on s5.PRODUCT_ID = coalesce(r5.salsify_product_id, p.salsify_product_id, d.salsify_product_id, c.salsify_product_id)
order by recharge_customer_id )

select *
from final
where current_gross_margin < 0.2
