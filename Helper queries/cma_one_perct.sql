with salsify_list as (select PARENT_ID,
       PRODUCT_ID,
       MANUFACTURER,
       WEB_SUPER_CATEGORY,
       WEB_CATEGORY,
       WEB_SUB_CATEGORY,
       VENDOR_ID as salsify_vendor_id,
       VENDOR_NAME as salsify_vendor_name,
       SHOPIFY_PRODUCT_ID,
       SHOPIFY_VARIANT_ID,
       UPC,
       asin,
       GP_SKU,
       VENDOR_ITEM_NUMBER,
       WEB_PRICE_MSRP,
       VAR_COST as current_salsify_cost,
       WEB_PRICE,
       round(((WEB_PRICE - VAR_COST)/WEB_PRICE),2) as gross_margin,
       LAST_PRICE_UPDATE
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where ((MANUFACTURER in ('DART',
'Konie',
'San Jamar',
'neo vas',
'Micropur',
'Genpak',
'Nite Ize',
'Fabri-Kal',
'Klean Kanteen') and WEB_CATEGORY = 'Accessories')
or (MANUFACTURER = 'EcoPlus' and WEB_CATEGORY = 'Aquarium Pumps & Supplies')
or (MANUFACTURER in ('American Granby','J.H. Verneco','Eco-Tech') and WEB_CATEGORY = 'Backflow Prevention')
or (MANUFACTURER in ('Coway','KX Industries') and WEB_CATEGORY = 'Carbon Filters')
or (MANUFACTURER = 'Doulton' and WEB_CATEGORY = 'Ceramic Filters & Cartridges')
or (MANUFACTURER = 'Flowmatic' and WEB_CATEGORY = 'Commercial & Industrial Filter Housings')
or (MANUFACTURER = 'JACO Manufacturing Company' and WEB_CATEGORY = 'Compression Fittings')
or (MANUFACTURER = 'Air Gap International' and WEB_CATEGORY = 'Drain Adapters')
or (MANUFACTURER = 'Elkay' and WEB_CATEGORY = 'Drinking Fountains')
or (MANUFACTURER in ('Doulton-British Berkefeld','Savant') and WEB_CATEGORY = 'Drinking Water Filtration Systems')
or (MANUFACTURER in ('Parker LIQUIFit','Touch-Flo') and WEB_CATEGORY = 'Faucet Accessories')
or (MANUFACTURER in ('Extech','MIRION Technologies','Hanna Instruments') and WEB_CATEGORY = 'Handheld Meters')
or (MANUFACTURER = 'Shelco' and WEB_CATEGORY = 'High Purity Filters')
or (MANUFACTURER in ('PurTest','Hach','Smart Brew') and WEB_CATEGORY = 'Home Test Kits & Strips')
or (MANUFACTURER = 'T&S Brass' and WEB_CATEGORY  = 'Hoses')
or (MANUFACTURER = 'Clear Water Technologies' and WEB_CATEGORY = 'Ice Filtration Systems')
or (MANUFACTURER = 'National Testing Laboratories, Ltd.' and WEB_CATEGORY = 'Lab Send-In Test Kits')
or (MANUFACTURER in ('ROPV','Hydrocomponents & Technologies, Inc.','Payne') and WEB_CATEGORY = 'Membrane Housings')
or (MANUFACTURER = 'Amway (Quixtar)' and WEB_CATEGORY = 'O-Rings')
or (MANUFACTURER = 'Harmsco' and WEB_CATEGORY = 'Pool & Spa Filters')
or (MANUFACTURER = 'Industrial Test Systems' and WEB_CATEGORY = 'Pool & Spa Test Kits')
or (MANUFACTURER = 'Gro Pro' and WEB_CATEGORY = 'Pots & Containers')
or (MANUFACTURER in ('Pressure Gauges','NOSHOK') and WEB_CATEGORY = 'Pressure Gauges')
or (MANUFACTURER in ('Norgren','Pressure Regulators','Neoperl') and WEB_CATEGORY = 'Pressure Regulators')
or (MANUFACTURER in ('Flojet','Little Giant') and WEB_CATEGORY = 'Pump Accessories')
or (MANUFACTURER = 'Parker' and WEB_CATEGORY = 'Quick-Connect Fittings')
or (MANUFACTURER in ('Kit','Aptera') and WEB_CATEGORY = 'Replacement Filters for RO Systems')
or (MANUFACTURER in ('Avalon','3M Purification Inc.') and WEB_CATEGORY = 'Replacement Water Filters & Cartridges')
or (MANUFACTURER = 'Seagull , General Ecology' and WEB_CATEGORY = 'Residential Water Filter Systems')
or (MANUFACTURER in ('Hydranautics','RainSoft') and WEB_CATEGORY = 'RO Membranes')
or (MANUFACTURER = 'VuFlow' and WEB_CATEGORY = 'Rusco Spin-Down & Sediment Traps')
or (MANUFACTURER in ('Better Water Industries','BECTON DICKINSON','Qosina') and WEB_CATEGORY = 'Sanitizing Kits & Media')
or (MANUFACTURER = 'Tuf-Tite' and WEB_CATEGORY = 'Septic')
or (MANUFACTURER in ('Rainshow''r','Micro Pure') and WEB_CATEGORY = 'Shower & Bath Water Filters')
or (MANUFACTURER in ('CFA','GC Valves','Burkert','Alcon','B/C Valve Company') and WEB_CATEGORY = 'Solenoid Valves')
or (MANUFACTURER  in ('Aries','Southeastern Filtration & Equipment Systems') and  WEB_CATEGORY = 'Specialty Cartridges')
or (MANUFACTURER in ('ShinMaywa','DURO','Matala USA') and WEB_CATEGORY = 'Sump Pumps')
or (MANUFACTURER in ('Waterstone Faucets','Square D','Flexeon') and WEB_CATEGORY = 'Switches')
or (MANUFACTURER in ('Zurn Industries','NewAge Industries') and WEB_CATEGORY = 'Tubing')
or (MANUFACTURER = 'AquaCera' and WEB_CATEGORY = 'Ultrafiltration Systems')
or (MANUFACTURER in ('QMP','SteriPEN','SIP Technologies','MicroFilter') and WEB_CATEGORY = 'UV Water Purification')
or (MANUFACTURER in ('SMC','Pureteck','CCK Automations') and WEB_CATEGORY = 'Valves')
or (MANUFACTURER in ('MegaMicrobes','Hydro Systems') and WEB_CATEGORY = 'Waste & Drain Cleaners')
or (MANUFACTURER in ('neo tote','Blender Bottle') and WEB_CATEGORY = 'Water Bottles')
or (MANUFACTURER in ('Dol-fyn','Pure Water') and WEB_CATEGORY = 'Water Distillers')
or (MANUFACTURER = 'G.A. Murdock' and WEB_CATEGORY = 'Water Supply Adapters')
or (MANUFACTURER = 'Enpress' and WEB_CATEGORY = 'Whole House Water Filtration'))
and NLADISPLAY_ID is null
and QUANTITY = 1
and IS_WEB_VARIANT_PUBLISHED = 1
and GP_SKU not ilike '%|%' ), --salsify data 1,492 products

gp_list as (select ITEM_NUMBER,
       VENDOR_ID,
       LAST_ORDER_DATE::date as LAST_ORDER_DATE,
       case when LAST_RECEIPT_COST = 0.00000 then LAST_ORIGINATING_COST else LAST_RECEIPT_COST end as last_order_cost,
       VENDOR_ITEM_DESCRIPTION,
       VENDOR_ITEM_NUMBER,
       dense_rank() over(partition by ITEM_NUMBER order by LAST_ORDER_DATE::date desc) as order_rank
from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS
where ITEM_NUMBER in (select GP_SKU from salsify_list)
order by 1 asc, 3 desc ), --raw vendor data

dupe_eliminator as (select *,
       count(*) over(partition by ITEM_NUMBER) as num_of_dupes
from gp_list
where order_rank = 1
order by 1 asc ), --clean vendor data

v_names as (select distinct trim(PRIMARY_VENDOR_ID) as primary_vendor_id,
       PRIMARY_VENDOR_NAME
from ANALYTICS.FWS_CORE.PRODUCTS ), --vendor names

vendor_data as (select ITEM_NUMBER,
       VENDOR_ID,
       PRIMARY_VENDOR_NAME,
       VENDOR_ITEM_NUMBER,
       VENDOR_ITEM_DESCRIPTION,
       LAST_ORDER_DATE,
       cost
from dupe_eliminator
left join v_names
on v_names.primary_vendor_id = trim(dupe_eliminator.VENDOR_ID)
where num_of_dupes = 1), --vendor data join


sales_data as (select ITEM_NUMBER,
       round(sum(nullif(EXTENDED_PRICE,0)),2) as past_year_revenue,
       round(sum(QUANTITY),1) as past_year_units_sold
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
and ITEM_NUMBER in (select GP_SKU from salsify_list)
group by 1
order by 2 asc ),   -- revenue and volume data

bc_raw as (select SITE_NAME,
       PRODUCT_URL,
       GP_SKU,
       PRICE as bc_comp_price,
       s1.PRODUCT_ID,
       row_number() over(partition by GP_SKU,SITE_NAME order by GP_SKU) as dupes
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES as bc
left join salsify_list s1
on bc.GTIN = lpad(s1.upc::int, 14, 0)
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
                                'WaterFiltersFAST.com'
                                )
and UPDATED_AT between dateadd('day', -14, current_date) and current_date
order by GP_SKU, PRICE ),

bc_rank as (select *,
       row_number() over(partition by GP_SKU order by bc_comp_price) as comp_rank
from bc_raw
where dupes = 1 ),

bc_comps as (select SITE_NAME,
       PRODUCT_URL,
       bc_comp_price,
       PRODUCT_ID,
       GP_SKU
from bc_rank
where comp_rank = 1 ),

keepa as (select asin as amz_asin,
       TITLE amz_title,
       SALES_RANK_CURRENT,
       NEW_CURRENT as keepa_comp_price,
       PRODUCT_CODES_PART_NUMBER,
       split_part(PRODUCT_CODES_UPC,',',1) as amz_upc,
       URL_AMAZON
from RAW.AMAZON_MANUAL.KEEPA_AMAZON_DATA
where len(NEW_CURRENT)>0 ),

sap_comps_raw as (select *,
       row_number() over(partition by SKU order by TOTAL_LOWEST_PRICE asc) as rank,
       row_number() over(partition by TOTAL_LOWEST_PRICE_MERCHANT,SKU order by TOTAL_LOWEST_PRICE_MERCHANT) as number
from raw.strike_a_price_s3_fws.google_shopping_daily
where SKU in (select GP_SKU from salsify_list)
  and _modified between dateadd('day', -31, current_date) and current_date
and MY_PRICE is not null
order by SKU asc, LOWEST_PRICE asc ),

sap_comps as (select ID,
       sku,
       gtin as sap_gtin,
       TOTAL_LOWEST_PRICE as sap_comp_price,
       TOTAL_LOWEST_PRICE_MERCHANT sap_site_name,
       LOWEST_SHIPPING
from sap_comps_raw
where rank =1 -- lowest price
and number =1 -- eliminate dupes
order by sku asc ),

google_benchmark as (select id,
       round(CURRENT_BENCHMARK_PRICE,2) as google_benchmark_price
from RAW.GOOGLE_MERCHANT_CENTER.GOOGLE_PRICE_COMP_30 )


-- final join
select s.*,
       v.*,
       sil.*,
       bc_comp_price,
       SITE_NAME as bc_site_name,
       PRODUCT_URL,
       amz_title,
       amz_upc,
       keepa_comp_price,
       URL_AMAZON,
       sap_comp_price,
       sap_site_name,
       google_benchmark_price
from salsify_list as s
left join vendor_data as v
    on v.ITEM_NUMBER = s.GP_SKU
left join sales_data as sil
    on sil.ITEM_NUMBER = s.GP_SKU
left join bc_comps
    on bc_comps.PRODUCT_ID = s.PRODUCT_ID
left join keepa k
    on k.amz_asin = s.ASIN
left join sap_comps
    on sap_comps.SKU = s.GP_SKU
left join google_benchmark as g
    on g.ID = s.SHOPIFY_VARIANT_ID
order by 2 asc
