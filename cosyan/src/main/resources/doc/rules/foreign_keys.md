# Use foreign keys to look up data.

<!-- RUN -->
```
create table customer (
  id integer,
  name varchar,
  age integer,
  constraint pk_id primary key (id));
```

<!-- RUN -->
```
create table transaction (
  category varchar,
  amount float,
  customer_id integer,
  constraint customer foreign key (customer_id) references customer(id),
  constraint c_adult check (not (category = 'alcohol') or customer.age >= 21));
```

<!-- RUN -->
```
insert into customer values (1, 'Adam', 25), (2, 'Bob', 16);
insert into transaction values ('food', 1.0, 2), ('alcohol', 1.0, 1);
```

<!-- TEST -->
```
select category, amount, customer.age from transaction;
```
```
category,amount,age
food,1.0,16
alcohol,1.0,25
```

<!-- ERROR -->
```
insert into transaction values ('alcohol', 1.0, 2);
```
```
Constraint check c_adult failed.
```
