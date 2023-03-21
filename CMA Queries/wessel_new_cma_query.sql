with item_type as (select item_number,it.ITEM_TYPE_NAME as item_type from ANALYTICS.FWS_STAGING.GP_MASTER_ITEMS i
inner join ANALYTICS.fws_staging.GP_ITEM_TYPE it on it.ITEM_TYPE_ID = i.ITEM_TYPE),

item_vendor_master as (
select item_number,max(LAST_ORIGINATING_COST) as last_cost,max(LAST_RECEIPT_DATE) as last_receipt from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS
group by ITEM_NUMBER
),

annual_sales as ( select ITEM_NUMBER,sum(ALLOCATED_DOCUMENT_SUBTOTAL) as sales FROM ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
WHERE SOP_TYPE = 'INVOICE'
AND VOID_STATUS <> 'Voided'
and DOCUMENT_DATE  BETWEEN DATEADD(Day,-365,current_date()) and current_date
group by ITEM_NUMBER),

vendor_cost_multi as (
select c.ITEM_NUMBER, max(VENDOR_MULTIPLIER) as VENDOR_MULTIPLIER ,max(VENDOR_LIST_PRICE) as VENDOR_LIST_PRICE ,
max(IS_SPA) as IS_SPA,max(COMMENTS) as COMMENTS,max(VENDOR_ID) as VENDOR_ID,max(LAST_MODIFIED_AT) as LAST_MODIFIED_AT ,
max(LAST_MODIFIED_USERNAME) as LAST_MODIFIED_USERNAME,max(last_cost) as last_cost,max(last_receipt) as last_receipt
from ANALYTICS.FWS_PRODUCT.VENDOR_INFO_COSTS c
left join item_vendor_master ic on ic.ITEM_NUMBER = c.ITEM_NUMBER
where VENDOR_STATUS = 'active' and nullif(trim(VENDOR_MULTIPLIER),'') is not null
group by c.ITEM_NUMBER
),

salsify_singles as (select product_id as s_product_id,s.product_name,
GP_SKU as gp_sku,QUANTITY,VENDOR_ITEM_NUMBER,lpad(UPC::int ,14, 0) as GTIN,ASIN as ASIN,
PRODUCT_PRICING_GROUP,
VAR_COST as current_cost,
last_cost as gp_last_cost,
v.IS_SPA as spa_pricing_gp,
coalesce((WEB_SALE_PRICE),(WEB_PRICE)) as web_price,
WEB_SALE_PRICE,
v.VENDOR_LIST_PRICE as gp_list_price,
WEB_PRICE_MSRP as web_list_price,
s.LIST_PRICE_YEAR as web_list_price_year,
s.MANUFACTURER,
s.VENDOR_ID as vendor_id,
v.VENDOR_ID as gp_vendor_id,
WEB_SUPER_CATEGORY,
WEB_CATEGORY,
WEB_SUB_CATEGORY,
IS_WEB_PRODUCT_PUBLISHED,
IS_WEB_VARIANT_PUBLISHED,
NLADISPLAY_ID,
last_receipt as gp_last_receipt,
v.VENDOR_MULTIPLIER as cost_multi,
item_type,
sales as annual_sales
From ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC s
    left join vendor_cost_multi v on s.GP_SKU = v.ITEM_NUMBER
    inner join item_type on item_type.ITEM_NUMBER = s.GP_SKU
    left join annual_sales sa on s.GP_SKU = trim(sa.ITEM_NUMBER)
where QUANTITY = 1
and MANUFACTURER ilike '%Watts%'),

bc_vendor_market_valid_14_d as (
select gp_sku,price,UPDATED_AT,s.gtin as gtin_id,SITE_NAME,SITE_ORDER,web_list_price,
(PRICE/web_list_price) as market_multi, IS_SPA,VENDOR_MULTIPLIER,bc.UPDATED_AT as market_price_updated_at,SEARCH_URL
from ANALYTICS.FWS_STAGING.BLACKCURVE_RECENT_COMPETITOR_PRICES bc
    inner join salsify_singles as s on bc.GTIN = s.GTIN
    inner join vendor_cost_multi c on c.ITEM_NUMBER = s.gp_sku
where (PRICE/web_list_price) > ((current_cost*1.05)/web_list_price)
and (PRICE/web_list_price) < 1.2
and UPDATED_AT > dateadd(day, -14, UPDATED_AT)),


bc_site_include_list as (select SITE_NAME,count(distinct(gtin_id)) from
bc_vendor_market_valid_14_d
group by SITE_NAME
order by count(*) desc limit 10),

mkt_data_by_segment as (select gp_sku,gtin_id,site_name,VENDOR_MULTIPLIER,price,web_list_price,market_price_updated_at,SEARCH_URL,IS_SPA,min(PRICE/web_list_price) over(partition by VENDOR_MULTIPLIER) as market_multi_by_vendor_cost_multi
From bc_vendor_market_valid_14_d l
    where SITE_NAME in (select SITE_NAME from bc_site_include_list)
)

--select * from mkt_data_by_segment order by gtin_id desc

select k.TITLE,gp_sku,ss.asin,GTIN,PRODUCT_CODES_PART_NUMBER,PRODUCT_CODES_UPC,SALES_RANK_CURRENT,SALES_RANK_REFERENCE,NEW_CURRENT,NEW_OFFER_COUNT_CURRENT,LIST_PRICE_CURRENT,LIST_PRICE_90_DAYS_AVG_,BUY_BOX_CURRENT,BUY_BOX_90_DAYS_AVG_,BUY_BOX_SELLER,BUY_BOX_CURRENT,
CATEGORIES_ROOT,CATEGORIES_SUB,CATEGORIES_TREE,RELEASE_DATE,FBA_PICK_PACK_FEE_FBA_PICK_PACK_FEE
 from raw.AMAZON_MANUAL.keepa_amazon_data k
    left join salsify_singles ss on  ss.asin = k.asin
where k.BRAND ilike '%Watts%'

--select * From raw.AMAZON_MANUAL.keepa_amazon_data limit 10

select * from salsify_singles ss
