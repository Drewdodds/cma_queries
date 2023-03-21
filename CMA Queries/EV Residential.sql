with bc as (select distinct
                SITE_NAME,
                PRODUCT_URL,
                PRICE as comp_price,
                GTIN
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES
where UPDATED_AT between dateadd(day, -14, UPDATED_AT) and current_date
      and SITE_NAME in ('Water Softeners & Filters',
                                'WaterFilters.NET',
                                'SupplyHouse.com',
                                'QC Supply',
                                'FiltersFast.com',
                                'SuperWater.com',
                                'KlearWaterStore.com',
                                'DiscountFilterStore.com',
                                'RestaurantSupply.com',
                                'H2O Distributors',
                                'efilters.net',
                                'Serv-A-Pure',
                                'Water Softeners & Filters',
                                'IceMachinesPLus.co',
                                'RestaurantTory',
                                'ABestKitchen Restaurant Equipment',
                                'WebstaurantStore.com',
                                'Walmart - DiscountFilterStore',
                                'Walmart - IPW Industries Inc.',
                                'Sears - Catch All',
                                'Walmart - AMI Ventures Inc',
                                'Sears'
                                )),

salsify_join as (select PRODUCT_ID,
       VENDOR_ID,
       lpad(UPC::int ,14, 0) as salsify_gtin,
       WEB_SUPER_CATEGORY,
       WEB_CATEGORY,
       WEB_SUB_CATEGORY,
       GP_SKU,
       PURCHASING_MULTIPLIER,
       SPA_PRICING,
       round(WEB_PRICE_MSRP,2) as List_price,
       round(VAR_COST,2) as cost,
       round((WEB_PRICE - cost) / WEB_PRICE, 2) as fws_profit_margin_pct,
       WEB_PRICE / nullif(List_price,0) as fws_retail_multiplier,
       WEB_PRICE,
       comp_price, -- need to add this to qualifier field
       comp_price / List_price as comp_retail_multiplier,
       round((WEB_PRICE - comp_price),2) as competitor_price_diff,
       round((comp_price - cost) / comp_price,2) as new_margin,
       round(avg(comp_price) over (partition by GP_SKU), 2) as avg_price_per_sku,
       case when WEB_PRICE > comp_price then 'FWS Above Comp'
            when WEB_PRICE < comp_price then 'FWS BELOW Comp'
            when comp_price is null then 'No Comp'
            else 'Same as Comp'
            end as competitor_comparison,
       case when WEB_PRICE > avg_price_per_sku then 'FWS Above Average'
            when WEB_PRICE < avg_price_per_sku then 'FWS BELOW Average'
            when avg_price_per_sku is null then 'No AVERAGE'
            else 'SAME as Average'
            end as avg_comparison,
       SITE_NAME,
       PRODUCT_URL,
       case when comp_retail_multiplier > 1 then 'Over List Price'
            when comp_retail_multiplier < .10 then '0.1'
            when comp_retail_multiplier is null then null
            else 'Comp in Range' end as comp_qualifier,
       dense_rank() over(partition by GP_SKU order by comp_price asc) as comp_rank
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s1
left join bc on bc.GTIN = lpad(s1.UPC::int ,14, 0)
where IS_WEB_VARIANT_PUBLISHED = 1
and NLADISPLAY_ID is null
and MANUFACTURER = 'Everpure Residential'
and GP_SKU not like '%PK%'
order by GP_SKU asc, SITE_NAME asc ),

sales_data as (select ITEM_NUMBER,
                      round(sum(nullif(EXTENDED_PRICE,0)),2) as past_year_revenue,
                      round(sum(EXTENDED_COST),2) as past_year_cost,
                      round(sum(QUANTITY),1) as past_year_units_sold,
                      past_year_revenue - past_year_cost as past_year_profit,
                      past_year_profit / past_year_revenue as past_year_margin
                from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
                where SOP_TYPE = 'INVOICE'
                and CANCELLATION_REASON is null
                and RETURN_REASON is null
                and DOCUMENT_DATE between dateadd('year', -1, current_date) and current_date
                and CUSTOMER_CLASS <> 'PHARMACY'
                and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
                and ITEM_DESCRIPTION not ilike '%refund%'
                and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
                and ITEM_NUMBER not in ('F-NON-INV-MISNTX' , 'F-MISNTX', 'F-FRGT', 'Test Item')
                group by 1
                order by 2 asc ),

super_cat as (select  WEB_SUPER_CATEGORY,
                       avg(comp_retail_multiplier) as super_cat_disc_multiplier
                from salsify_join
                where comp_price is not null
                and comp_qualifier = 'Comp in Range'
                group by 1 ),

cat as (select  WEB_CATEGORY,
                avg(comp_retail_multiplier) as cat_disc_multiplier
            from salsify_join
            where comp_price is not null
            and comp_qualifier = 'Comp in Range'
            group by 1 ),

sub_cat as (select  WEB_SUB_CATEGORY as sub_category,
                    avg(comp_retail_multiplier) as sub_cat_disc_multiplier
                from salsify_join
                where comp_price is not null
                and comp_qualifier = 'Comp in Range'
                group by 1 ),

 market_position as (select GP_SKU,
                           competitor_comparison,
                           count(*) as price_match_per_category,
                           sum(price_match_per_category) over(partition by GP_SKU) as total_price_matches,
                           round(price_match_per_category / total_price_matches,2) as market_position_1,
                           round((1- market_position_1),2) as market_position_2
                    from salsify_join
                    group by 1,2
                    order by 1 ),

final_join as (select s.*,
       past_year_revenue,
       past_year_profit,
       past_year_margin,
       past_year_units_sold,
       case when comp_retail_multiplier is not null and comp_qualifier = 'Comp in Range' then round(comp_retail_multiplier,10)
            when comp_retail_multiplier is null or comp_qualifier <> 'Comp in Range' then
            coalesce(
                    round(sub_cat_disc_multiplier, 10),
                    round(cat_disc_multiplier, 10),
                    round(super_cat_disc_multiplier, 10),
                coalesce(super_cat_disc_multiplier, 0)
                    )
        end as market_retail_multiplier,
       case when comp_retail_multiplier is not null and comp_qualifier = 'Comp in Range' then 'SKU'
            when sub_cat_disc_multiplier is not null then 'Sub Category'
            when cat_disc_multiplier is not null then 'Category'
            when super_cat_disc_multiplier is not null then 'Super Category'
            when super_cat_disc_multiplier is null then 'Manual'
       end as retail_multiplier_type,
       total_price_matches,
       case when s.competitor_comparison = 'FWS Above Comp' then market_position_2 * 100
            when s.competitor_comparison = 'FWS BELOW Comp' then market_position_1 * 100
            when s.competitor_comparison = 'Same as Comp' then market_position_2 * 100
       end as below_market_percetage
from salsify_join as s
left join sales_data sil on sil.ITEM_NUMBER = s.GP_SKU
left join super_cat on super_cat.WEB_SUPER_CATEGORY = s.WEB_SUPER_CATEGORY
left join cat on cat.WEB_CATEGORY = s.WEB_CATEGORY
left join sub_cat on sub_cat.sub_category = s.WEB_SUB_CATEGORY
left join market_position m on m.GP_SKU = s.GP_SKU
order by GP_SKU asc),


duplicate_tag as (select *,
       row_number() over(partition by GP_SKU order by SITE_NAME) as number
from final_join
where comp_rank = 1
order by GP_SKU asc )
-- filters for the bottom of the market (sometimes two comps can be ranked #1)

select *
from duplicate_tag
where number = 1
order by competitor_price_diff asc
-- filters for just one of the #1 comps above if there happen to be two
