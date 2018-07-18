### Use reverse foreign keys to reference multiple records

Foreign keys define a many to one connection, but every foreign key implicitly
defines a reverse one to many connection as well. This can be exploited to define
views and rules for aggregates.

Let's create a product type table.
<!-- RUN -->
```
create table product_type (
  id integer,
  name varchar,
  constraint pk_id primary key (id));
```

There is a many to one mapping from products to product types. The reverse one to many
connection can be named via the `reverse` keyword. 
<!-- RUN -->
```
create table product (
  id integer,
  type_id integer not null,
  name varchar,
  constraint pk_id primary key (id),
  constraint type foreign key (type_id) references product_type(id) reverse products);
```

Similarly, one transaction has exactly one product (and transitively product_type), and
one product can have multiple corresponding transactions.
<!-- RUN -->
```
create table transaction (
  product_id integer not null,
  price float,
  constraint product foreign key (product_id) references product(id) reverse transactions);
```

We can use the `aggref` keyword to define an aggregated view of a one to many connection. This
subtable and its columns can be referred similarly to foreign keys via its name.
<!-- RUN -->
```
alter table product add aggref stats (
  select count(1) as cnt, sum(price) as sum_price from transactions);
```

Similarly, we can build multiple levels of aggregations using a chain of one to many connections.
E.g. `sum_price` here refers to the total price of all transactions of all products of any given
product_type.
<!-- RUN -->
```
alter table product_type add aggref stats (
  select sum(stats.cnt) as cnt, sum(stats.sum_price) as sum_price from products);
```

We can define constraints for aggregates. Let's say we have a limited inventory for every
product_type, we cannot have more than 5 transactions.
<!-- RUN -->
```
alter table product_type add constraint c_cnt check (stats.cnt <= 5);
```

Let's say we want to offer a discount but only if we sold enough products already. The price can
only be less than 100 if we sold at least 1000 worth of that specific product type.
<!-- RUN -->
```
alter table transaction add constraint c_discount check (
  price < 100 impl product.type.stats.sum_price >= 1000);
```

Let's add some records.
<!-- RUN -->
```
insert into product_type values (1, 'toy');
insert into product values (1, 1, 'doll'), (2, 1, 'truck');
insert into transaction values (1, 300.0), (1, 300.0), (2, 300.0);
```

We can query the aggregated view of one to many connections simply by using the name of the
`aggref` table.
<!-- TEST -->
```
select name, stats.cnt, stats.sum_price from product_type;
```
```
name,cnt,sum_price
toy,3,900.0
```

Since the total price of toys is only 900, we cannot sell one for 50.
<!-- ERROR -->
```
insert into transaction values (1, 50.0);
```
```
Constraint check c_discount failed.
```

But if we push the overall price to 1000, it works.
<!-- RUN -->
```
insert into transaction values (1, 100.0);
insert into transaction values (1, 50.0);
```

We have reached the overall number of 5 transactions for toys (via 5 successful and 1
unsuccessful inserts), so we cannot add more.
<!-- ERROR -->
```
insert into transaction values (2, 50.0);
```
```
Referencing constraint check product_type.c_cnt failed.
```

The `parent` keyword can be used to refer to the source table in an `aggref`. 
<!-- RUN -->
```
alter table product add aggref truck_stats (
  select count(1) as cnt, sum(price) as sum_price from transactions where parent.name = 'truck');
```

Now the aggregated table is only computed for "trucks".
<!-- TEST -->
```
select name, truck_stats.cnt, truck_stats.sum_price from product;
```
```
name,cnt,sum_price
doll,0,null
truck,1,300.0
```

