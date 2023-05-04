with dataforseo as (select SCRAPED_AT,
                           GTIN,
       SELLER_NAME,
       TITLE,
       BASE_PRICE,
       SELLER_PRODUCT_URL,
       GOOGLE_SHOPPING_URL
from ANALYTICS.FWS_STAGING.DATAFORSEO_LATEST_SELLERS
where SELLER_NAME in ('Water Softeners & Filters',
                                    'WaterFilters.NET',
                                    'SupplyHouse.com',
                                    'QC Supply',
                                    'FiltersFast.com',
                                    'Filters.com',
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
                                    'American Copper & Brass',
                                    'High Pressure Pumps & Parts',
                                    'Toboa Energy Resources',
                                    'CLR Marine',
                                    'Isopure Reverse Osmosis Water Filters',
                                    'Vita Filters',
                                     'iFilters',
                                     'affordablewater.us',
                                     'ProLampSales')
and SCRAPED_AT between dateadd('month', -1, current_date) and current_date
order by GTIN )

select SCRAPED_AT,
       d.GTIN,
       SELLER_NAME,
       TITLE,
       BASE_PRICE,
       SELLER_PRODUCT_URL,
       GOOGLE_SHOPPING_URL,
       PRODUCT_ID,
       WEB_PRICE,
       row_number() over(partition by d.GTIN,SELLER_NAME order by SCRAPED_AT desc) as most_recent_scrape
from dataforseo d
left join ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC as s
    on s.GTIN = d.GTIN
qualify most_recent_scrape = 1
order by GTIN, SELLER_NAME