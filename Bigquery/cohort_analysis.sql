with orders as (
  select 
      *,
      case
          when @date_grain = 'week' then date(date_trunc(created_at,week))
          when @date_grain = 'month' then date(date_trunc(created_at,month))
          when @date_grain = 'quarter' then date(date_trunc(created_at,quarter))
      end as order_created_period,
      case
          when @date_grain = 'week' then min(date(date_trunc(created_at,month))) over (partition by user_id order by created_at)
          when @date_grain = 'month' then min(date(date_trunc(created_at,month))) over (partition by user_id order by created_at)
          when @date_grain = 'quarter' then min(date(date_trunc(created_at,quarter))) over (partition by user_id order by created_at)
      end as first_purchase_period,
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
        case
          when @date_grain = 'week' then date_diff(order_created_period,first_purchase_period,week)
          when @date_grain = 'month' then date_diff(order_created_period,first_purchase_period,month)
          when @date_grain = 'quarter' then date_diff(order_created_period,first_purchase_period,quarter)
        end as purchase_period,
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
where purchase_period != 0
