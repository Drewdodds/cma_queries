select SITE_NAME,
       PRODUCT_URL,
       PRICE as comp_price,
       GTIN,
       s.PRODUCT_ID,
       GP_SKU,
       row_number() over(partition by GP_SKU,SITE_NAME order by GP_SKU) as dupes,
       row_number() over(partition by GP_SKU order by comp_price) as comp_rank
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES as bc
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s
on bc.GTIN = s.VENDOR_ITEM_NUMBER
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
                                'GetPoolParts.com'
                                )
and bc.UPDATED_AT between dateadd('day', -14, current_date) and current_date
and GP_SKU in ()
qualify dupes = 1 and comp_rank = 1
order by GP_SKU, PRICE
