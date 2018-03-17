# Use reverse foreign keys to reference multiple records

<!-- RUN -->
```
create table product_type (
  id integer,
  name varchar,
  constraint pk_id primary key (id));
```

<!-- RUN -->
```
create table product (
  id integer,
  type_id integer not null,
  name varchar,
  constraint pk_id primary key (id),
  constraint type foreign key (type_id) references product_type(id) reverse products);
```

<!-- RUN -->
```
create table transaction (
  product_id integer not null,
  price float,
  constraint product foreign key (product_id) references product(id) reverse transactions);
```

<!-- RUN -->
```
alter table product add ref stats (
  select count(1) as cnt, sum(price) as sum_price from transactions);
```

<!-- RUN -->
```
alter table product_type add ref stats (
  select sum(stats.cnt) as cnt, sum(stats.sum_price) as sum_price from products);
```
<!-- RUN -->
```
alter table product_type add constraint c_cnt check (stats.cnt <= 5);
```
<!-- RUN -->
```
alter table transaction add constraint c_discount check (
  price >= 100 or product.type.stats.sum_price >= 1000);
```

<!-- RUN -->
```
insert into product_type values (1, 'toy');
insert into product values (1, 1, 'doll'), (2, 1, 'truck');
insert into transaction values (1, 300.0), (1, 300.0), (2, 300.0);
```

<!-- TEST -->
```
select name, stats.cnt, stats.sum_price from product_type;
```
```
name,cnt,sum_price
toy,3,900.0
```

<!-- ERROR -->
```
insert into transaction values (1, 50.0);
```
```
Constraint check c_discount failed.
```

<!-- RUN -->
```
insert into transaction values (1, 100.0);
insert into transaction values (1, 50.0);
```

<!-- ERROR -->
```
insert into transaction values (2, 50.0);
```
```
Referencing constraint check product_type.c_cnt failed.
```