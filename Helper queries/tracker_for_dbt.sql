with bc as (select bc.GTIN,
       GP_SKU as item_number,
       SITE_NAME as seller_name,
       PRICE,
       date_trunc(day, bc.UPDATED_AT)::date as updated_at
from {{ ref('blackcurve_competitor_prices') }} as bc
--from ANALYTICS.FWS_STAGING.BLACKCURVE_COMPETITOR_PRICES as bc
left join {{ ref('static_salsify_basic') }} as sal
--left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as sal
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

dataforseo as (select ds.GOOGLE_PRODUCT_ID,
       gpid.KEYWORD as gtin,
       ds.SELLER_NAME,
       ds.BASE_PRICE as price,
       date_trunc(day, SCRAPED_AT)::date as updated_at
from {{ ref('google_product_ids') }} as gpid
--from ANALYTICS.FWS_STAGING.GOOGLE_PRODUCT_IDS as gpid
left join {{ ref('dataforseo_seller') }} as ds
--left join analytics.fws_staging.dataforseo_sellers as ds
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

dfseo_sal as (select dfseo.gtin,
       sal2.GP_SKU as item_number,
       dfseo.SELLER_NAME,
       dfseo.price,
       dfseo.updated_at
from dataforseo as dfseo
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as sal2
    on sal2.GTIN = dfseo.gtin
where sal2.UPC is not null
and sal2.QUANTITY = 1 ),


fws as (select ITEM_NUMBER,
               'FWS'                                as seller_name,
               round(UNIT_PRICE, 2)                 as price,
               date_trunc(day, DOCUMENT_DATE)::Date as updated_at
        from {{ ref('sales_item_level') }}
        --from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
        where SOP_TYPE = 'INVOICE'
          and CANCELLATION_REASON is null
          and ORDER_SOURCE = 'FWS Web'
          and array_to_string(ORDER_TAGS, ',') not like '%Subscription%' -- filters out subscription orders
          and RETURN_REASON is null
          and DOCUMENT_DATE >= '2020-06-29' --first day bc data started coming through
          and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
          and ITEM_DESCRIPTION not ilike '%refund%'
          and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
          and ITEM_NUMBER not in ('F-NON-INV-MISNTX', 'F-MISNTX', 'F-FRGT', 'Test Item')
),

combined_results as (
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
    from fws
    )

select *
from combined_results
order by updated_at asc, item_number