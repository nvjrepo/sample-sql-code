with orders as (
  select 
      *,
      date(date_trunc(created_at,month)) as order_created_month,
      min(date(date_trunc(created_at,month))) over (partition by user_id order by created_at) as first_purchase_month,
      row_number() over (partition by user_id order by created_at) as customer_orders
  from `bigquery-public-data.thelook_ecommerce.orders`
),

monthly_cohort_size as (

    select
        first_purchase_month,
        count(distinct user_id) as customers

    from orders

    where customer_orders=1
    group by 1
),

cohort_revenue as (

    select
        first_purchase_month,
        date_diff(order_created_month,first_purchase_month,month) as purchase_month,
        count(distinct user_id) as retained_customers        
    from orders
    group by 1,2

)

select 
    cohort_revenue.*,
    monthly_cohort_size.customers

from cohort_revenue
left join monthly_cohort_size
    on cohort_revenue.first_purchase_month = monthly_cohort_size.first_purchase_month
order by 1,2
