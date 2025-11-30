create table customer (
    customer_id int primary key,
    first_name varchar(100),
    last_name varchar(100),
    gender varchar(10),
    dob date,
    job_title varchar(100),
    job_industry_category varchar(100),
    wealth_segment varchar(50),
    deceased_indicator varchar(10),
    owns_car varchar(10),
    address varchar(200),
    postcode varchar(20),
    state varchar(50),
    country varchar(50),
    property_valuation int
);

create table product (
    product_id int primary key,
    brand varchar(100),
    product_line varchar(100),
    product_class varchar(50),
    product_size varchar(50),
    list_price decimal(10,2),
    standard_cost decimal(10,2)
);

create table orders (
    order_id int primary key,
    customer_id int,
    order_date date,
    online_order boolean,
    order_status varchar(50),
    foreign key (customer_id) references customer(customer_id)
);

create table order_items (
    order_item_id int primary key,
    order_id int,
    product_id int,
    quantity int,
    item_list_price_at_sale decimal(10,2),
    item_standard_cost_at_sale decimal(10,2),
    foreign key (order_id) references orders(order_id),
    foreign key (product_id) references product(product_id)
);

truncate table product cascade;

create table product_temp as select * from product where 1=0;

insert into product_temp select * from product;

delete from product_temp 
where ctid not in (
    select min(ctid) 
    from product_temp 
    group by product_id
);

insert into product 
select * from product_temp;

drop table product_temp;

create table orders_temp (
    order_id int,
    customer_id int,
    order_date date,
    online_order boolean,
    order_status varchar(50)
);

insert into orders (order_id, customer_id, order_date, online_order, order_status)
select ot.order_id, ot.customer_id, ot.order_date, ot.online_order, ot.order_status
from orders_temp ot
where ot.customer_id in (select customer_id from customer);

-- проверка пропущенных заказов
select * from orders_temp 
where customer_id not in (select customer_id from customer);

select count(*) as total_orders from orders_temp;
select count(*) as imported_orders from orders;
select count(*) as skipped_orders 
from orders_temp 
where customer_id not in (select customer_id from customer);

create table order_items_temp (
    order_item_id int,
    order_id int,
    product_id int,
    quantity decimal(10,2),
    item_list_price_at_sale decimal(10,2),
    item_standard_cost_at_sale decimal(10,2)
);

insert into order_items (
    order_item_id, order_id, product_id, quantity, 
    item_list_price_at_sale, item_standard_cost_at_sale
)
select 
    order_item_id, 
    order_id, 
    product_id,
    quantity,
    item_list_price_at_sale,
    item_standard_cost_at_sale
from order_items_temp
where order_id in (select order_id from orders)
  and product_id in (select product_id from product);

drop table orders_temp;
drop table order_items_temp;
-- запрос 1: распределение клиентов по сферам деятельности
select 
    job_industry_category,
    count(*) as customer_count
from customer
group by job_industry_category
order by customer_count desc;

-- запрос 2: общий доход по месяцам и сферам деятельности
select 
    extract(year from o.order_date) as year,
    extract(month from o.order_date) as month,
    c.job_industry_category,
    sum(oi.item_list_price_at_sale * oi.quantity) as total_revenue
from orders o
join customer c on o.customer_id = c.customer_id
join order_items oi on o.order_id = oi.order_id
where o.order_status = 'Approved'
group by 
    extract(year from o.order_date),
    extract(month from o.order_date),
    c.job_industry_category
order by year, month, c.job_industry_category;

-- запрос 3: уникальные онлайн-заказы для брендов от IT-клиентов
select 
    p.brand,
    count(distinct case when o.online_order = true then o.order_id end) as online_orders_count
from product p
left join order_items oi on p.product_id = oi.product_id
left join orders o on oi.order_id = o.order_id
left join customer c on o.customer_id = c.customer_id
where (c.job_industry_category = 'IT' or c.job_industry_category is null)
    and (o.order_status = 'Approved' or o.order_status is null)
group by p.brand
order by online_orders_count desc;

-- запрос 4: статистика по клиентам (group by)
select 
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(oi.item_list_price_at_sale * oi.quantity) as total_revenue,
    max(oi.item_list_price_at_sale * oi.quantity) as max_order_amount,
    min(oi.item_list_price_at_sale * oi.quantity) as min_order_amount,
    count(o.order_id) as orders_count,
    avg(oi.item_list_price_at_sale * oi.quantity) as avg_order_amount
from customer c
left join orders o on c.customer_id = o.customer_id
left join order_items oi on o.order_id = oi.order_id
group by c.customer_id, c.first_name, c.last_name
order by total_revenue desc nulls last, orders_count desc nulls last;

-- запрос 5: топ-3 мин и макс сумм транзакций
with customer_totals as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        coalesce(sum(oi.item_list_price_at_sale * oi.quantity), 0) as total_revenue
    from customer c
    left join orders o on c.customer_id = o.customer_id
    left join order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name
),
min_max_ranks as (
    select 
        customer_id,
        first_name,
        last_name,
        total_revenue,
        row_number() over (order by total_revenue asc) as min_rank,
        row_number() over (order by total_revenue desc) as max_rank
    from customer_totals
)
select 
    customer_id,
    first_name,
    last_name,
    total_revenue,
    'Min' as type
from min_max_ranks
where min_rank <= 3
union all
select 
    customer_id,
    first_name,
    last_name,
    total_revenue,
    'Max' as type
from min_max_ranks
where max_rank <= 3
order by type, total_revenue;

-- запрос 6: вторые транзакции клиентов
with ordered_orders as (
    select 
        o.customer_id,
        o.order_id,
        o.order_date,
        row_number() over (partition by o.customer_id order by o.order_date) as order_rank
    from orders o
)
select 
    oo.customer_id,
    c.first_name,
    c.last_name,
    oo.order_id,
    oo.order_date
from ordered_orders oo
join customer c on oo.customer_id = c.customer_id
where oo.order_rank = 2;

-- запрос 7: максимальный интервал между заказами
with order_intervals as (
    select 
        o.customer_id,
        c.first_name,
        c.last_name,
        c.job_title,
        o.order_date,
        lead(o.order_date) over (partition by o.customer_id order by o.order_date) as next_order_date,
        lead(o.order_date) over (partition by o.customer_id order by o.order_date) - o.order_date as days_between
    from orders o
    join customer c on o.customer_id = c.customer_id
),
max_intervals as (
    select 
        customer_id,
        first_name,
        last_name,
        job_title,
        max(days_between) as max_interval_days
    from order_intervals
    where days_between is not null
    group by customer_id, first_name, last_name, job_title
)
select 
    customer_id,
    first_name,
    last_name,
    job_title,
    max_interval_days
from max_intervals
order by max_interval_days desc;

-- запрос 8: топ-5 клиентов по доходу в каждом сегменте
with customer_wealth_rank as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        coalesce(sum(oi.item_list_price_at_sale * oi.quantity), 0) as total_revenue,
        row_number() over (partition by c.wealth_segment order by coalesce(sum(oi.item_list_price_at_sale * oi.quantity), 0) desc) as wealth_rank
    from customer c
    left join orders o on c.customer_id = o.customer_id
    left join order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, c.wealth_segment
)
select 
    customer_id,
    first_name,
    last_name,
    wealth_segment,
    total_revenue,
    wealth_rank
from customer_wealth_rank
where wealth_rank <= 5
order by wealth_segment, wealth_rank;
