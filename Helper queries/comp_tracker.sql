with combined_prices as (
                select
                        google_product_id,
                        gtin,
                        SITE_NAME as seller_name,
                        price,
                        date_trunc(day, updated_at)::date as updated_at
                        --'BlackCurve' as source
                    from analytics.fws_staging.blackcurve_competitor_prices
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
                    qualify row_number() over(partition by google_product_id,GTIN,seller_name,UPDATED_AT order by UPDATED_AT desc)=1


    union

            select
                    ds.google_product_id,
                    gpid.keyword as gtin,
                    ds.seller_name as seller_name,
                    ds.base_price as price,
                    date_trunc(day, ds.scraped_at)::date as updated_at
                   --'DataForSEO' as source
                from analytics.fws_staging.google_product_ids gpid
                left join analytics.fws_staging.dataforseo_sellers ds
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
                qualify row_number() over(partition by gpid.google_product_id,GTIN,seller_name,UPDATED_AT order by UPDATED_AT desc)=1
)

select cp.GOOGLE_PRODUCT_ID,
       cp.GTIN,
       sal.GP_SKU,
       cp.seller_name,
       cp.PRICE as competitor_price,
       cp.updated_at
from combined_prices as cp
join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as sal
    on sal.GTIN = cp.GTIN
where QUANTITY = 1
and UPC is not null
order by updated_at asc
