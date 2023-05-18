with bc as (select SITE_NAME,
       PRODUCT_URL,
       PRICE as comp_price,
       bc.GTIN,
       s.PRODUCT_ID,
       GP_SKU,
       bc.UPDATED_AT,
       row_number() over(partition by GP_SKU,SITE_NAME order by bc.UPDATED_AT desc) as dupes
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES as bc
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s
    on bc.GTIN = s.GTIN
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
                                'High Pressure Pumps & Parts',
                                'Toboa Energy Resources',
                                'CLR Marine',
                                'Vita Filters',
                                'iFilters',
                                'affordablewater.us',
                                'ProLampSales',
                                'FactoryPure',
                                'Rainwater Management Solutions')
and bc.UPDATED_AT between dateadd('day', -14, current_date) and current_date
--and GP_SKU in ()
and s.QUANTITY = 1
qualify dupes = 1
order by GP_SKU, SITE_NAME asc )

select *,
       row_number() over(partition by GP_SKU order by comp_price asc) as comp_rank
from bc
qualify comp_rank = 1
order by GP_SKU asc, SITE_NAME asc

