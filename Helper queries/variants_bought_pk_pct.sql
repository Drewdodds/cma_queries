with order_list as (select ORIGINAL_NUMBER
from ANALYTICS.FWS_CORE.SALES_ITEM_LEVEL
where SOP_TYPE = 'INVOICE'
and CANCELLATION_REASON is null
and RETURN_REASON is null
and DOCUMENT_DATE between '2021-08-31'and '2022-08-31' --hard coded date for a specific timestamp
and ITEM_TYPE_DESC not in ('misc charge', 'flat fee')
and ITEM_DESCRIPTION not ilike '%refund%'
and lower(ITEM_DESCRIPTION) not ilike 'CANCELLED-DISCONTINUED%'
and ITEM_NUMBER not in ('F-NON-INV-MISNTX' , 'F-MISNTX', 'F-FRGT', 'Test Item')),

sku_list as (select UPDATED_TIMESTAMP,
       shop_1.ORDER_ID,
       shop_1.name as order_number,
       shop_2.SKU,
       trim(split_part(shop_2.SKU, ':', 1)) as base_sku
from ANALYTICS.FWS_STAGING.STG_SHOPIFY__ORDER as shop_1
left join ANALYTICS.FWS_STAGING.STG_SHOPIFY__ORDER_LINE as shop_2 on shop_1.ORDER_ID = shop_2.ORDER_ID
where shop_1.name in (select ORIGINAL_NUMBER from order_list)
and shop_2.SKU ilike '%:%PK%' ),

pk_skus as (select sku as pk_sku,
                   base_sku,
       count(*) as num_of_pk_sku_purchased
from sku_list
group by 1,2 ),

single_sku as (select UPDATED_TIMESTAMP,
       shop_1.ORDER_ID,
       shop_1.name as order_number,
       shop_2.SKU
from ANALYTICS.FWS_STAGING.STG_SHOPIFY__ORDER as shop_1
left join ANALYTICS.FWS_STAGING.STG_SHOPIFY__ORDER_LINE as shop_2 on shop_1.ORDER_ID = shop_2.ORDER_ID
where shop_1.name in (select ORIGINAL_NUMBER from order_list)
and shop_2.SKU not ilike '%:%PK%' ),

single_skus as (select sku as single_sku,
       count(*) as num_of_single_sku_purchased
from single_sku as sing
group by 1 ),

sku_join as (select single_sku,
       pk_sku,
       num_of_single_sku_purchased,
       num_of_pk_sku_purchased,
       num_of_pk_sku_purchased + num_of_single_sku_purchased as total_instances,
       num_of_pk_sku_purchased / total_instances as pct_pk_bought,
       case when num_of_pk_sku_purchased >= num_of_single_sku_purchased then 1 else 0 end as pk_won,
       case when num_of_single_sku_purchased > num_of_pk_sku_purchased  then 1 else 0 end as single_won
from single_skus as si
join pk_skus as pk   on pk.base_sku = si.single_sku )

select count(*) as total_records,
       sum(pk_won) as sum_pk_won,
       sum(single_won) as sum_single_won,
       sum_pk_won / total_records as pct_pk_won
from sku_join