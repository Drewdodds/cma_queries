select id,
       TITLE,
       round(CURRENT_BENCHMARK_PRICE,2) as google_benchmark_price
from RAW.GOOGLE_MERCHANT_CENTER.GOOGLE_PRICE_COMP_30
where CURRENT_BENCHMARK_PRICE is not null
and ID in ()
