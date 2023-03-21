with singles as (select PRODUCT_ID,
       GP_SKU as single_sku,
       WEB_PRICE as single_price,
       WEB_PRICE_MSRP as single_msrp,
       VAR_COST as single_cost,
       QUANTITY as single_qty,
       MANUFACTURER as single_manufacturer,
       LAST_PRICE_UPDATE as single_last_price_update
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where NLADISPLAY_ID is null
and IS_WEB_VARIANT_PUBLISHED = 1
and QUANTITY = 1),

qtys as (select PRODUCT_ID,
       GP_SKU,
       trim(split_part(GP_SKU, ':', 1)) as single_ref,
       WEB_PRICE as current_price,
       WEB_PRICE_MSRP as current_msrp,
       VAR_COST as current_cost,
       QUANTITY::int as Quantity,
       MANUFACTURER,
       count(*) over(partition by single_ref)+1 as num_of_vars
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where NLADISPLAY_ID is null
and IS_WEB_VARIANT_PUBLISHED = 1
and QUANTITY > 1
order by 2 asc),

variant_discounts as (select qtys.*,
       single_price,
       single_cost,
       single_msrp,
     dense_rank() over(partition by single_ref order by qtys.QUANTITY asc ) as var_rank,
     case when num_of_vars = 2 then round(((single_price-(single_price*.05))*qtys.QUANTITY),2)
          when num_of_vars = 3 and var_rank = 1 then round(((single_price-(single_price*0.025))*QUANTITY),2)
          when num_of_vars = 3 and var_rank = 2 then round(((single_price-(single_price*0.05))*QUANTITY),2)
          when num_of_vars = 4 and var_rank = 1 then round(((single_price-(single_price*0.016666667))*QUANTITY),2)
          when num_of_vars = 4 and var_rank = 2 then round(((single_price-(single_price*0.033333333))*QUANTITY),2)
          when num_of_vars = 4 and var_rank = 3 then round(((single_price-(single_price*0.05))*QUANTITY),2)
          when num_of_vars = 5 and var_rank = 1 then round(((single_price-(single_price*0.0125))*QUANTITY),2)
          when num_of_vars = 5 and var_rank = 2 then round(((single_price-(single_price*0.025))*QUANTITY),2)
          when num_of_vars = 5 and var_rank = 3 then round(((single_price-(single_price*0.0375))*QUANTITY),2)
          when num_of_vars = 5 and var_rank = 4 then round(((single_price-(single_price*0.05))*QUANTITY),2)
          when num_of_vars = 6 and var_rank = 1 then round(((single_price-(single_price*0.01))*QUANTITY),2)
          when num_of_vars = 6 and var_rank = 2 then round(((single_price-(single_price*0.02))*QUANTITY),2)
          when num_of_vars = 6 and var_rank = 3 then round(((single_price-(single_price*0.03))*QUANTITY),2)
          when num_of_vars = 6 and var_rank = 4 then round(((single_price-(single_price*0.04))*QUANTITY),2)
          when num_of_vars = 6 and var_rank = 5 then round(((single_price-(single_price*0.05))*QUANTITY),2)
    end as new_price,
       round((single_cost * QUANTITY),2) as new_cost,
       case when single_msrp is null then null else round((QUANTITY * single_msrp),2) end as new_msrp,
       round((single_price * QUANTITY),2) as no_disc_price,
       case when new_price < no_disc_price then 'Good' else 'Bad' end as new_price_check
from qtys
join singles
    on singles.single_sku = qtys.single_ref
order by single_ref asc ,QUANTITY asc ),

final as (select *
from variant_discounts
where new_price_check = 'Good'
order by single_ref asc, QUANTITY asc),


bs_compare as (select s1.PRODUCT_ID,
       s1.MANUFACTURER,
       s1.GP_SKU,
       trim(split_part(s1.GP_SKU, ':', 1)) as single_ref_2,
       s1.QUANTITY,
       var_rank,
       LAST_PRICE_UPDATE as qty_last_price_update,
       WEB_PRICE,
       new_price,
       final.current_price,
       case when WEB_PRICE <> new_price then 'No Match' else 'Match' end as bc_check
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s1
join final
    on final.PRODUCT_ID = s1.PRODUCT_ID
where PRICE_MANAGER = 'Blackcurve - Pack' )

select bs_compare.PRODUCT_ID,
       bs_compare.GP_SKU,
       single_ref_2,
       var_rank,
       bs_compare.QUANTITY,
       qty_last_price_update,
       single_last_price_update,
       current_price,
       bs_compare.WEB_PRICE,
       new_price,
       bc_check
from bs_compare
join singles
    on singles.single_sku = bs_compare.single_ref_2
