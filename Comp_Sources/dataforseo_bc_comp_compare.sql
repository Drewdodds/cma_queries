with dataforseo_list as (select *,
       row_number() over(partition by GOOGLE_PRODUCT_ID, SELLER_NAME order by SCRAPED_AT desc) as update_rank
from dev.dev_csnyder_pricing.DATAFORSEO_SELLERS
qualify  update_rank = 1 ),

dataforseo_matches as (select GOOGLE_PRODUCT_ID,
       count(*) dataforseo_matches_per_id
from dataforseo_list
group by 1 ),

bc_list as (select GOOGLE_PRODUCT_ID,
       row_number() over(partition by GOOGLE_PRODUCT_ID, SITE_NAME order by UPDATED_AT desc) as date_rank
from ANALYTICS.FWS_STAGING.BLACKCURVE_COMPETITOR_PRICES
where IS_GTIN_SCRAPED <> 'false'
and GOOGLE_PRODUCT_ID is not null
and len(GTIN) = 14
qualify date_rank = 1
order by SITE_NAME asc ),

bc_matches as (select GOOGLE_PRODUCT_ID,
       count(*) bc_matches_per_id
from bc_list
group by 1 ),

match_join as (select d.GOOGLE_PRODUCT_ID,
       dataforseo_matches_per_id,
       bc_matches_per_id,
       case when dataforseo_matches_per_id > bc_matches_per_id then 1 else 0 end as dataforseo_wins,
       case when bc_matches_per_id > dataforseo_matches_per_id then 1 else 0 end as bc_wins
from dataforseo_matches as d
join bc_matches b on b.GOOGLE_PRODUCT_ID = d.GOOGLE_PRODUCT_ID
group by 1,2,3
order by 1 asc )

select count(*) total_records,
       sum(dataforseo_wins) data_for_seo,
       sum(bc_wins) bc
from match_join
