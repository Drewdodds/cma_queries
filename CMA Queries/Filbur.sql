with bc as (select GTIN,
                   PRICE as comp_price,
                   SITE_NAME,
                   PRODUCT_URL
            from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES
            where UPDATED_AT between dateadd(day, -14, current_date) and current_date),


s1 as (select PRODUCT_ID,
                   lpad(upc::int, 14, 0) as salsify_gtin,
                   VENDOR_ID,
                   GP_SKU,
                   WEB_SUPER_CATEGORY,
                   WEB_CATEGORY,
                   round(VAR_COST,2) as fws_cost,
                   round((WEB_PRICE - VAR_COST)/WEB_PRICE, 2) as fws_gross_margin,
                   WEB_PRICE,
                   comp_price,
                   round(WEB_PRICE - comp_price,2) as competitor_price_diff,
                   case when WEB_PRICE > comp_price then 'fws above comp'
                        when WEB_PRICE < comp_price then 'fws below comp'
                        when WEB_PRICE = comp_price then 'same as comp'
                        when  comp_price is null then 'no comp price' end as competitor_comparison,
                    round(avg(comp_price) over (partition by GP_SKU),2) as avg_price_per_sku,
                   case when WEB_PRICE > avg_price_per_sku then 'fws above avg'
                        when WEB_PRICE < avg_price_per_sku then 'fws below average'
                        when WEB_PRICE = avg_price_per_sku then 'same as average'
                        when  comp_price is null then 'no avg price' end as avg_comparison,
                    SITE_NAME,
                    PRODUCT_URL,
                    row_number() over(partition by SITE_NAME, GP_SKU order by SITE_NAME) as number -- used to exclude duplicates
            from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as salsify
            left join bc on bc.GTIN = lpad(salsify.UPC::int, 14, 0)
            where salsify.manufacturer = 'Filbur'
            and QUANTITY = 1
            and WEB_VARIANT_PUBLISHED = 1
            order by GP_SKU, comp_price), -- Combines bc data with salsify

top_comps as (select SITE_NAME, -- top 20 competitors based on sku matches
                       count(*)
                from s1
                where number = 1 -- gets rid of duplicates
                group by 1
                order by 2 desc
                limit 20),

comp_join as (select *
                from s1
                where SITE_NAME in (select SITE_NAME from top_comps)
                and number = 1 ), -- excludes duplicates

spectrum as (select    GP_SKU, -- gets the below market percentage position per sku
                       competitor_comparison as cc,
                       count(*) as number_of_prices_per_category,
                       sum(number_of_prices_per_category) over(partition by GP_SKU) as total_price_matches,
                       round(number_of_prices_per_category / total_price_matches,2) as market_position_1,
                       round((1- market_position_1),2)  as market_position_2
                from comp_join
                group by 1,2
                order by 1 asc),

margin_rank as (select ITEM_NUMBER,
                       (sum(EXTENDED_PRICE) - sum(extended_cost)) / nullif(sum(extended_price),0) as margin_pct_rank
                from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
                where DOCUMENT_DATE between dateadd(year, -1, current_Date) and current_date
                and SOP_TYPE = 'INVOICE'
                group by 1
                order by 2 asc ),

revenue_rank as (select ITEM_NUMBER,
                       sum(EXTENDED_PRICE) as revenue_rank
                from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
                where DOCUMENT_DATE between dateadd(year, -1, current_Date) and current_date
                and SOP_TYPE = 'INVOICE'
                group by 1
                order by 2 desc ),

volume_rank as (select ITEM_NUMBER,
                       count(*) as volume_rank
                from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
                where DOCUMENT_DATE between dateadd(year, -1, current_Date) and current_date
                and SOP_TYPE = 'INVOICE'
                group by 1
                order by 2 desc ),

final_join as (select PRODUCT_ID,
                   salsify_gtin,
                   s1.GP_SKU,
                   fws_cost,
                   fws_gross_margin,
                   WEB_PRICE,
                   comp_price,
                   competitor_price_diff,
                round((comp_price - fws_cost)/comp_price,2) as new_margin,
                   s1.competitor_comparison,
                    total_price_matches,
       case when cc = 'fws above comp' then market_position_2 * 100
            when cc =  'fws below comp' then market_position_1 * 100
            when cc = 'same as comp' then  market_position_2 * 100
            end as below_market_percentage,
                   round(avg(comp_price) over(partition by s1.GP_SKU),2) as new_avg_comp_per_sku, -- based on new comp list from comp_join cte
                   case when WEB_PRICE >  new_avg_comp_per_sku then 'fws above avg'
                        when WEB_PRICE < new_avg_comp_per_sku then 'fws below average'
                        when WEB_PRICE = new_avg_comp_per_sku then 'same as average'
                        when  comp_price is null then 'no avg price' end as new_avg_comparison,
                   SITE_NAME,
                   PRODUCT_URL,
                   margin_pct_rank,
                   revenue_rank,
                   volume_rank,
                   number,
         dense_rank() over(partition by s1.GP_SKU order by comp_price asc) as comp_rank
            from s1
            join spectrum on spectrum.GP_SKU = s1.GP_SKU and spectrum.cc = s1.competitor_comparison
            left join margin_rank m on m.ITEM_NUMBER = s1.GP_SKU
            left join volume_rank v on v.ITEM_NUMBER = s1.GP_SKU
            left join revenue_rank r on r.ITEM_NUMBER = s1.GP_SKU
            where number = 1
            and SITE_NAME in (select top_comps.SITE_NAME from top_comps)
            order by s1.GP_SKU, comp_price)

select *
from final_join
where comp_rank = 1
order by below_market_percentage asc
