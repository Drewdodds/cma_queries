with sku_list as (select PARENT_ID,
       PRODUCT_ID,
       GP_SKU,
       split_part(GP_SKU,':',1) as single_ref,
       QUANTITY::int as quantity,
       min(quantity) over (partition by parent_id) as min_quantity,
       max(quantity) over (partition by parent_id) as max_quantity,
       round(DISCOUNT_SPREAD,5) as discount_spread,
       iff(PRICE_MANAGER='Map', 0,
           round(coalesce(DISCOUNT_SPREAD, 0.05) * percent_rank() over(partition by PARENT_ID order by QUANTITY::int asc),5)) as disc_off_of_single,
       percent_rank() over(partition by PARENT_ID order by QUANTITY::int asc) as ratio_spread,
       WEB_PRICE_MSRP,
       WEB_PRICE,
       WEB_MAP_PRICE,
       round(VAR_COST,5) as var_cost,
       PRICE_MANAGER,
       count(*) over(partition by PARENT_ID) as num_of_vars,
       row_number() over(partition by PARENT_ID order by QUANTITY::int asc) as var_rank
--from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
from {{ ref('static_salsify_basic') }}
where NLADISPLAY_ID is null
and PARENT_ID in (select PARENT_ID
                  from {{ ref('static_salsify_basic') }}
                  --from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
                  where QUANTITY>1)
and IS_WEB_VARIANT_PUBLISHED = 1
order by PARENT_ID, QUANTITY asc ),

singles as (select PRODUCT_ID,
                        PARENT_ID as single_parent_id,
                        GP_SKU as single_sku,
                        WEB_PRICE as single_price,
                        WEB_PRICE_MSRP as single_msrp,
                        round(VAR_COST,5) as single_cost,
                        WEB_MAP_PRICE as single_map_price
                from sku_list
                where QUANTITY = 1),

qtys as (select PRODUCT_ID,
                PARENT_ID as qty_parent_id,
                GP_SKU,
                single_ref,
                WEB_PRICE as current_price,
                WEB_PRICE_MSRP as current_msrp,
                VAR_COST as current_cost,
                WEB_MAP_PRICE as current_map_price,
                QUANTITY,
                DISCOUNT_SPREAD,
                disc_off_of_single,
                num_of_vars,
                var_rank,
                PRICE_MANAGER as price_manager,
                WEB_MULTIPLIER as web_multiplier,
                iff(PRICE_MANAGER='Map', 0, round((quantity - min_quantity) * ifnull(discount_spread,0) / (max_quantity - min_quantity),5)) as qty_based_discount
        from sku_list
        where QUANTITY > 1
        order by 2 asc),

variant_discounts as (select q.*,
       single_price,
       single_msrp,
       single_map_price,
       case when (discount_spread = 0 or PRICE_MANAGER='Map')then round((single_price * quantity),2)
            when PRICE_MANAGER='Manual' then round((single_price * quantity) * (1 - disc_off_of_single),2)
            when PRICE_MANAGER='Multiplier' then round((WEB_MULTIPLIER * (single_msrp*quantity)),2)
            end as new_price,
       case when discount_spread = 0 then round((single_price * quantity),2)
            else round((single_price * quantity) * (1 - qty_based_discount),2)
            end as qty_based_disc_price,
       case when single_msrp is null then null
            else round((single_msrp * quantity),2)
            end as new_msrp,
       case when single_map_price is null then null
            else round((single_map_price * quantity),2)
            end as new_map_price,
       round((single_cost * quantity),2) as new_cost
from qtys as q
join singles as s
    on s.single_sku = q.single_ref
order by single_ref, quantity asc )

select *
from variant_discounts
order by single_ref asc, quantity asc