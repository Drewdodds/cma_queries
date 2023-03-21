select PARENT_ID,
       PRODUCT_ID,
       MANUFACTURER,
       WEB_SUPER_CATEGORY,
       WEB_CATEGORY,
       WEB_SUB_CATEGORY,
       VENDOR_ID,
       VENDOR_NAME,
       SHOPIFY_PRODUCT_ID,
       SHOPIFY_VARIANT_ID,
       UPC,
       asin,
       GP_SKU,
       VENDOR_ITEM_NUMBER,
       WEB_PRICE_MSRP,
       VAR_COST as current_salsify_cost,
       WEB_PRICE,
       round(((WEB_PRICE - VAR_COST)/WEB_PRICE),2) as gross_margin
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where NLADISPLAY_ID is null
and IS_WEB_VARIANT_PUBLISHED = 1
and QUANTITY = 1
