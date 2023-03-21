with bc as (select distinct
                SITE_NAME,
                PRODUCT_URL,
case when gtin = '00054568589200' then round((price / 24),2) -- filtersfast.com match -- matches with qty packs
     when gtin = '00054568553324' then round((price / 6),2) -- filtersfast.com match -- matches with qty packs
     else PRICE end as comp_price,
                GTIN
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES
where UPDATED_AT between dateadd(day, -14, UPDATED_AT) and current_date
and gtin not in ('00211711922912', '00054568550835', '00054568551061') --- bad matches
      and SITE_NAME in ('Water Softeners & Filters',
                                'WaterFilters.NET',
                                'SupplyHouse.com',
                                'QC Supply',
                                'FiltersFast.com',
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
                                'Walmart - AMI Ventures Inc'
                              )
    and PRODUCT_URL not in
        ('www.google.com/aclk?sa=L&ai=DChcSEwiCuLPM_7vzAhWFbG8EHSI0Dd0YABAJGgJqZg&sig=AOD64_1SuMzb14eOuIDuJvv8uFAZq6K2pg&ctype=5&q=&ved=0ahUKEwiHvbDM_7vzAhVLWs0KHQILCGsQ1ikINQ&adurl=',
         'www.google.com/url?q=http://www.klearwaterstore.com/everpure-i2000-2-insurice-ev9612-22-filter-cartridge/&sa=U&ved=0ahUKEwjoj4i6r7zzAhU7Ap0JHd_NCwEQ1ykISA&usg=AOvVaw0WQrx_OhEXPKdOQ3uDfiNy',
         'www.google.com/url?q=https://www.efilters.net/products/everpure-i20002-water-filter-cartridge-1-pk-ev961222%3Fcurrency%3DUSD%26variant%3D18038683992182%26utm_medium%3Dcpc%26utm_source%3Dgoogle%26utm_campaign%3DGoogle%2520Shopping&sa=U&ved=0ahUKEwjoj4i6r7zzAhU7Ap0JHd_NCwEQ1ykIRA&usg=AOvVaw1FbacvlL5bVjdCVCaT2c2H',
         'www.google.com/aclk?sa=L&ai=DChcSEwiU-9q4serzAhVIcW8EHYwOAwwYABANGgJqZg&sig=AOD64_29IDayvBKBko-uFgw0fxi649jekg&ctype=5&q=&ved=0ahUKEwjL7te4serzAhWQQc0KHT1mAJAQ1ikIOg&adurl=',
         'www.google.com/aclk?sa=L&ai=DChcSEwi_0LaGlffzAhVFJzgKHWC3BmgYABABGgJqZg&sig=AOD64_3HEPhZNdFhyPXp1Ukd12H__HowzQ&ctype=5&q=&ved=0ahUKEwiS47OGlffzAhVWa80KHRNqD4oQ1ikIPQ&adurl=',
         'www.google.com/url?q=http://www.klearwaterstore.com/everpure-ev9328-01-coldrink-1-mc2-system/&sa=U&ved=0ahUKEwju5u_qjOvzAhXFB80KHdshAngQ1ykIXg&usg=AOvVaw0veAKixl2z4MXg04TqiDgL'
        )
    ), -- wrong product matches

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
       WEB_PRICE / List_price as fws_retail_multiplier,
       WEB_PRICE,
       comp_price,
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
            when comp_retail_multiplier < .10 then '<0.1'
            when comp_retail_multiplier is null then null
            else 'Comp in Range' end as comp_qualifier,
    iff(PRODUCT_ID = '55271v', 'Pentair-EVC-IFS', PRODUCT_PRICING_GROUP) as PRODUCT_PRICING_GROUP,
       dense_rank() over(partition by GP_SKU order by comp_price asc) as comp_rank
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s1
left join bc on bc.GTIN = lpad(s1.UPC::int ,14, 0)
where IS_WEB_VARIANT_PUBLISHED = 1
and GP_SKU not like '%:%PK' -- filter out qtys skus this way instead of qty field b/c some skus are a set
and MANUFACTURER = 'Everpure Commercial'
and NLADISPLAY_ID is null
and PRODUCT_ID <> '50455v' -- already priced to market
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
                and DOCUMENT_DATE between dateadd('year', -1, DOCUMENT_DATE) and current_date
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
                and comp_rank = 1
                group by 1 ),

cat as (select  WEB_CATEGORY,
                avg(comp_retail_multiplier) as cat_disc_multiplier
            from salsify_join
            where comp_price is not null
             -- and comp_rank = 1
            and comp_qualifier = 'Comp in Range'
            group by 1 ),

sub_cat as (select  WEB_SUB_CATEGORY as sub_category,
                    avg(comp_retail_multiplier) as sub_cat_disc_multiplier
                from salsify_join
                where comp_price is not null
                  --and comp_rank = 1
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
        end as market_retail_multiplier, -- this only takes into account retail multipliers that were in range
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
order by GP_SKU asc ),
-- filters for the bottom of the market (sometimes two comps can be ranked #1)

final_list as (select *
from duplicate_tag
where number = 1 ),

fws_disc as (select PRODUCT_PRICING_GROUP,
       avg(PURCHASING_MULTIPLIER) as avg_fws_cost_multiplier,
       avg(fws_retail_multiplier) as avg_fws_retail_multiplier
from final_list
group by 1 ),

comp_disc as (select PRODUCT_PRICING_GROUP,
       avg(comp_retail_multiplier) as avg_comp_retail_multiplier
from final_list
where comp_rank = 1
and comp_qualifier = 'Comp in Range'
group by  1 )

select t1.*,
       avg_comp_retail_multiplier,
       avg_fws_retail_multiplier - avg_fws_cost_multiplier as current_spread,
       avg_comp_retail_multiplier - avg_fws_cost_multiplier as future_spread
from fws_disc as t1
join comp_disc as t2
    on t1.PRODUCT_PRICING_GROUP = t2.PRODUCT_PRICING_GROUP
