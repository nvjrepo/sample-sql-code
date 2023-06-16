with orders as (
  select 
      *,
      date(date_trunc(created_at,month)) as order_created_month,
      min(date(date_trunc(created_at,month))) over (partition by user_id order by created_at) as first_purchase_period,
      row_number() over (partition by user_id order by created_at) as customer_orders
  from `bigquery-public-data.thelook_ecommerce.orders`
),

cohort_size as (

    select
        first_purchase_period,
        count(distinct user_id) as customers

    from orders

    where customer_orders=1
    group by 1
),

cohort_revenue as (

    select
        first_purchase_period,
        date_diff(order_created_month,first_purchase_period,month) as purchase_month,
        count(distinct user_id) as retained_customers        
    from orders
    group by 1,2

)

select 
    cohort_revenue.*,
    cohort_size.customers

from cohort_revenue
left join cohort_size
    on cohort_revenue.first_purchase_period = cohort_size.first_purchase_period
order by 1,2
