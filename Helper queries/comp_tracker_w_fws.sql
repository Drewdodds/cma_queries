with bc as (select bc.GTIN, --bc data
       GP_SKU as item_number,
       SITE_NAME as seller_name,
       PRICE,
       date_trunc(day, bc.UPDATED_AT)::date as updated_at
from ANALYTICS.FWS_STAGING.BLACKCURVE_COMPETITOR_PRICES as bc
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as sal
    on bc.GTIN = sal.GTIN
where SITE_NAME in ('Water Softeners & Filters',
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
                    'Sears',
                    'Zoro',
                    'U.S. Plastic Corporation',
                    'Parts Town',
                    'PlumbersStock.com',
                    'ESPWaterProducts.com',
                    'Safe Water Essentials',
                    'WaterFiltersFAST.com',
                    'KitchenRestock.com',
                    'Hotel Restaurant Supply',
                    'PartsGopher',
                    'SpaDepot.com',
                    'Wild West Pool Supplies',
                    'Hot Tub Warehouse',
                    'MasterSpaParts.com',
                    'GetPoolParts.com',
                    'WECO Filters',
                    'Restaurant Equipment World',
                    'High Pressure Pumps & Parts',
                    'Kleen-Rite Corp',
                    'American Copper & Brass')
and sal.QUANTITY = 1
and sal.upc is not null
qualify row_number() over(partition by bc.GTIN,seller_name, GP_SKU, bc.UPDATED_AT order by bc.UPDATED_AT desc)=1 --eleminates dupes
    order by bc.UPDATED_AT asc, seller_name asc
    ),

dataforseo as (select ds.GOOGLE_PRODUCT_ID, -- data for seo data
       gpid.KEYWORD as gtin,
       ds.SELLER_NAME,
       ds.BASE_PRICE as price,
       date_trunc(day, SCRAPED_AT)::date as updated_at
from ANALYTICS.FWS_STAGING.GOOGLE_PRODUCT_IDS as gpid
left join analytics.fws_staging.dataforseo_sellers as ds
    on ds.google_product_id = gpid.google_product_id
where SELLER_NAME in ('Water Softeners & Filters',
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
                    'Sears',
                    'Zoro',
                    'U.S. Plastic Corporation',
                    'Parts Town',
                    'PlumbersStock.com',
                    'ESPWaterProducts.com',
                    'Safe Water Essentials',
                    'WaterFiltersFAST.com',
                    'KitchenRestock.com',
                    'Hotel Restaurant Supply',
                    'PartsGopher',
                    'SpaDepot.com',
                    'Wild West Pool Supplies',
                    'Hot Tub Warehouse',
                    'MasterSpaParts.com',
                    'GetPoolParts.com',
                    'WECO Filters',
                    'Restaurant Equipment World',
                    'High Pressure Pumps & Parts',
                    'Kleen-Rite Corp',
                    'American Copper & Brass')
qualify row_number() over(partition by ds.GOOGLE_PRODUCT_ID,seller_name, gtin, UPDATED_AT order by UPDATED_AT desc)=1--eleminates dupes
                    ),

dfseo_sal as (select dfseo.gtin, -- data for seo data combined with salsify
       sal2.GP_SKU as item_number,
       dfseo.SELLER_NAME,
       dfseo.price,
       dfseo.updated_at
from dataforseo as dfseo
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as sal2
    on sal2.GTIN = dfseo.gtin
where sal2.UPC is not null
and sal2.QUANTITY = 1 ),


fws_web_price as (select ITEM_NUMBER, --fws data
                         'FWS Web' as seller_name,
               round(UNIT_PRICE, 2) as price,
               date_trunc(day, DOCUMENT_DATE)::Date as updated_at
        from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
        where SOP_TYPE = 'INVOICE'
          and CANCELLATION_REASON is null
          and ORDER_SOURCE = 'FWS Web'
          and UNIT_PRICE > 0 -- takes out records where the order the old pipe sku for kits
          and (array_to_string(ORDER_TAGS, ',') not like '%Subscription%Subscription Recurring Order%' or ORDER_TAGS is null) -- filters out subscription recurring orders and keeps the rest
          and RETURN_REASON is null
          and DOCUMENT_DATE >= '2020-06-29' --first day bc data started coming through
          and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
          and ITEM_DESCRIPTION not ilike '%refund%'
          and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
          and ITEM_NUMBER not in ('F-NON-INV-MISNTX', 'F-MISNTX', 'F-FRGT', 'Test Item')
),
     fws_amz_price as (select ITEM_NUMBER, --fws data
                         'FWS AMZ' as seller_name,
               round(UNIT_PRICE, 2) as price,
               date_trunc(day, DOCUMENT_DATE)::Date as updated_at
        from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
        where SOP_TYPE = 'INVOICE'
          and CANCELLATION_REASON is null
          and ORDER_SOURCE ='Amazon Marketplace'
          and RETURN_REASON is null
          and DOCUMENT_DATE >= '2020-06-29' --first day bc data started coming through
          and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
          and ITEM_DESCRIPTION not ilike '%refund%'
          and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
          and ITEM_NUMBER not in ('F-NON-INV-MISNTX', 'F-MISNTX', 'F-FRGT', 'Test Item')
         ),

fws_cost as (select ITEM_NUMBER,
               'FWS Cost' as seller_name,
               case when UNIT_OF_MEASURE_QTY_IN_BASE = 1 then UNIT_COST
                    when UNIT_OF_MEASURE_QTY_IN_BASE > 1 then round((UNIT_COST / UNIT_OF_MEASURE_QTY_IN_BASE),2)
                    end as price,
               PROMISED_DATE::Date as updated_at
from ANALYTICS.FWS_STAGING.GP_PURCHASE_ORDER_LINES as gp
where updated_at >= '2020-06-29'),

combined_results as ( -- stacked the above records from the CTEs above on top of each other while eliminating dupes if present
        select item_number,
               seller_name,
               PRICE,
               updated_at
        from bc
    union
        select item_number,
               seller_name,
               PRICE,
               updated_at
        from dfseo_sal
    union
        select item_number,
               seller_name,
               PRICE,
               updated_at
        from fws_web_price
    union
        select item_number,
               seller_name,
               PRICE,
               updated_at
        from fws_amz_price
    union
        select ITEM_NUMBER,
               seller_name,
               price,
               updated_at
        from fws_cost
    )

select *
from combined_results
where item_number = '3MROM413'
order by updated_at asc