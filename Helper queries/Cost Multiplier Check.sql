with vendor_cost_multi as (
                            select ITEM_NUMBER,
                                   max(VENDOR_MULTIPLIER) as VENDOR_MULTIPLIER,
                                   max(VENDOR_LIST_PRICE) as VENDOR_LIST_PRICE,
                                   max(IS_SPA) as IS_SPA,
                                   max(COMMENTS) as COMMENTS,
                                   max(VENDOR_ID) as VENDOR_ID,
                                   max(LAST_MODIFIED_AT) as LAST_MODIFIED_AT ,
                                   max(LAST_MODIFIED_USERNAME) as LAST_MODIFIED_USERNAME
                            from ANALYTICS.FWS_PRODUCT.VENDOR_INFO_COSTS c
                            where ITEM_TYPE <> 'Discontinued' and VENDOR_STATUS = 'active' and nullif(trim(VENDOR_MULTIPLIER),'') is not null
                            --and VENDOR_ID = 'N-PENTAIRSHURFL'
                            group by ITEM_NUMBER
),
salsify_singles as (select product_id as s_product_id,
                           s.product_name,
                           GP_SKU as gp_sku,
                           QUANTITY,
                           VENDOR_ITEM_NUMBER,
                           lpad(UPC::int ,14, 0) as GTIN,
                           ASIN as ASIN,
                           PRODUCT_PRICING_GROUP,
                           VAR_COST as current_cost,
                           v.IS_SPA as spa_pricing_gp,
                           coalesce((WEB_SALE_PRICE),(WEB_PRICE)) as web_price,
                           WEB_SALE_PRICE,
                           v.VENDOR_LIST_PRICE as gp_list_price,
                           WEB_PRICE_MSRP as web_list_price,
                           s.LIST_PRICE_YEAR as web_list_price_year,
                           s.VENDOR_ID as vendor_id,
                           v.VENDOR_ID as gp_vendor_id,
                           WEB_SUPER_CATEGORY,
                           WEB_CATEGORY,
                           WEB_SUB_CATEGORY,
                           IS_WEB_PRODUCT_PUBLISHED,
                           IS_WEB_VARIANT_PUBLISHED
                            From ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC s
                                left join vendor_cost_multi v on s.GP_SKU = v.ITEM_NUMBER
                            where QUANTITY = 1
                              and NLADISPLAY_ID is null
                              and IS_WEB_VARIANT_PUBLISHED = 1
                            and MANUFACTURER = 'Everpure Residential') -- insert vendor or manufacturer

select * from salsify_singles
