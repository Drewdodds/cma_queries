with t1 as (select ITEM_NUMBER,
       VENDOR_ID,
       LAST_ORDER_DATE::date as LAST_ORDER_DATE,
       case when LAST_RECEIPT_COST = 0.00000 then LAST_ORIGINATING_COST else LAST_RECEIPT_COST end as cost,
       VENDOR_ITEM_DESCRIPTION,
       VENDOR_ITEM_NUMBER,
       dense_rank() over(partition by ITEM_NUMBER order by LAST_ORDER_DATE::date desc) as order_rank
from ANALYTICS.FWS_STAGING.GP_ITEM_MASTER_VENDORS --also use this table to match vendor_ids to salsufy ids as well as a reference as to who we last bought from and how much.
where ITEM_NUMBER in ()
order by 1 asc, 3 desc ),

t2 as (select *,
       count(*) over(partition by ITEM_NUMBER) as num_of_dupes
from t1
where order_rank = 1
order by 1 asc ),

v_names as (select distinct trim(PRIMARY_VENDOR_ID) as primary_vendor_id,
       PRIMARY_VENDOR_NAME
from ANALYTICS.FWS_CORE.PRODUCTS )

select ITEM_NUMBER,
       VENDOR_ID,
       PRIMARY_VENDOR_NAME,
       VENDOR_ITEM_NUMBER,
       VENDOR_ITEM_DESCRIPTION,
       LAST_ORDER_DATE,
       cost
from t2
left join v_names
on v_names.primary_vendor_id = trim(t2.VENDOR_ID)
where num_of_dupes = 1
