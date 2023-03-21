with item_vendor_master as (select item_number,
       max(LAST_ORIGINATING_COST) as last_cost,
       max(LAST_RECEIPT_DATE) as last_receipt
from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS
group by ITEM_NUMBER)
-- last cost


select c.ITEM_NUMBER,
       max(VENDOR_MULTIPLIER) as VENDOR_MULTIPLIER,
       max(VENDOR_LIST_PRICE) as VENDOR_LIST_PRICE,
       max(IS_SPA) as IS_SPA,max(COMMENTS) as COMMENTS,
       max(VENDOR_ID) as VENDOR_ID,
       max(LAST_MODIFIED_AT) as LAST_MODIFIED_AT ,
       max(LAST_MODIFIED_USERNAME) as LAST_MODIFIED_USERNAME,
       max(last_cost) as last_cost,
       max(last_receipt) as last_receipt
from ANALYTICS.FWS_PRODUCT.VENDOR_INFO_COSTS c
left join item_vendor_master ic on ic.ITEM_NUMBER = c.ITEM_NUMBER
where VENDOR_STATUS = 'active' and nullif(trim(VENDOR_MULTIPLIER),'') is not null
group by c.ITEM_NUMBER
--latest pricing data from vendors
