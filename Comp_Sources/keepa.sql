select asin,
       TITLE,
       SALES_RANK_CURRENT,
       NEW_CURRENT,
       PRODUCT_CODES_PART_NUMBER,
       split_part(PRODUCT_CODES_UPC,',',1) as upc,
       URL_AMAZON,
       NUMBER_OF_ITEMS
from RAW.AMAZON_MANUAL.KEEPA_AMAZON_DATA
where ASIN in ()
and len(NEW_CURRENT)>0
