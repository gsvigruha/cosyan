# Use foreign keys to reference lookup tables

Let's create a customer table:
<!-- RUN -->
```
create table customer (
  id integer,
  name varchar,
  age integer,
  constraint pk_id primary key (id));
```

Let's create a transaction table with a foreign key to the customer table and a rule
testing that only adults buy alcohol.
<!-- RUN -->
```
create table transaction (
  category varchar,
  amount float,
  customer_id integer,
  constraint customer foreign key (customer_id) references customer(id),
  constraint c_adult check (category = 'alcohol' impl customer.age >= 21));
```

Let's add two customers, one adult and one under age.
<!-- RUN -->
```
insert into customer values (1, 'Adam', 25), (2, 'Bob', 16);
insert into transaction values ('food', 1.0, 2), ('alcohol', 1.0, 1);
```

Querying the table shows that both transactions are added. Columns from the customer
table can be referred directly using the `customer` foreign key.
<!-- TEST -->
```
select category, amount, customer.age from transaction;
```
```
category,amount,age
food,1.0,16
alcohol,1.0,25
```

Adding a transaction with under age Bob buying alcohol should fail.
<!-- ERROR -->
```
insert into transaction values ('alcohol', 1.0, 2);
```
```
Constraint check c_adult failed.
```