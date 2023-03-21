with parent_ids as (select PARENT_ID
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where PARENT_ID in (select PARENT_ID from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC where QUANTITY>1)
    ),

singles as (select PRODUCT_ID,
                   GP_SKU,
       IS_IN_STOCK,
       HAS_OPEN_PO,
       NEXT_EXPECTED_RECEIPT_DATE,
       HYDRIAN_LEADTIME_90_TH_DAYS,
       HYDRIAN_LEADTIME_MEDIAN_DAYS
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where PARENT_ID in (select parent_ids.PARENT_ID from parent_ids)
and QUANTITY = 1
    ),
qty as (select PRODUCT_ID,
       GP_SKU,
      trim(split_part(GP_SKU, ':', 1)) as base_sku
from ANALYTICS.FWS_STAGING.STATIC_SALSIFY_BASIC
where PARENT_ID in (select parent_ids.PARENT_ID from parent_ids)
and QUANTITY > 1 )

select q.*,
       s.HYDRIAN_LEADTIME_MEDIAN_DAYS,
       s.HYDRIAN_LEADTIME_90_TH_DAYS,
       s.NEXT_EXPECTED_RECEIPT_DATE,
       s.HAS_OPEN_PO,
       s.IS_IN_STOCK
from qty q 
join singles s 
    on s.GP_SKU = q.base_sku