### Use foreign keys to reference lookup tables

First let's create a membership table.
<!-- RUN -->
```
create table membership (
  id integer,
  type varchar,
  constraint pk_id primary key (id));
```

Then a customer table referencing the accounts. In this scenario the customer does not
need to have an account.
<!-- RUN -->
```
create table customer (
  id integer,
  name varchar,
  age integer,
  membership_id integer,
  constraint pk_id primary key (id),
  constraint membership foreign key (membership_id) references membership(id));
```

Finally let's create a transaction table with a foreign key to the customer table and two
rules testing that
  1) only adults buy alcohol,
  2) expensive purchases are only allowed with a gold membership.
<!-- RUN -->
```
create table transaction (
  category varchar,
  amount float,
  customer_id integer not null,
  constraint customer foreign key (customer_id) references customer(id),
  constraint c_adult check (category = 'alcohol' impl customer.age >= 21),
  constraint c_has_membership check (amount >= 100 impl customer.membership.type is not null),
  constraint c_gold_membership check (amount >= 100 impl customer.membership.type = 'gold'));
```
For the second rule we add two constraints, one checking that the customer _has_ an account
and one to check the account type. This is needed because of the way SQL handles expressions
with `null`s in them. Another way to handle this with one constraint would be
`amount >= 100 impl case when customer.membership.type is null then false else customer.membership.type = 'gold' end`.

Let's add two customers with some transactions, one adult and one under age. Neither of them
has a membership. In this case the `c_gold_membership` rule evaluates as `null` which does
not trigger an error by default.
<!-- RUN -->
```
insert into customer values (1, 'Adam', 25, null), (2, 'Bob', 16, null);
insert into transaction values ('food', 1.0, 2), ('alcohol', 1.0, 1);
```

Querying the table shows that both transactions are added. Corresponding data from the
customer table can be referred directly using the `customer` foreign key.
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

We should now create a customers with memberships.
<!-- RUN -->
```
insert into membership values (1, 'gold'), (2, 'silver');
insert into customer values (3, 'Cecil', 30, 1), (4, 'Dave', 50, 2);
```

Add a transaction for gold member Cecil with a high amount:
<!-- RUN -->
```
insert into transaction values ('toy', 100.0, 3);
```

We can query again the transactions with their respective customers and accounts:
<!-- TEST -->
```
select category, amount, customer.name, customer.membership.type as membership_type from transaction;
```
```
category,amount,name,membership_type
food,1.0,Bob,null
alcohol,1.0,Adam,null
toy,100.0,Cecil,gold
```

However, the following transaction should trigger the `c_gold_membership` constraint, since
Dave does not have a gold membership.
<!-- ERROR -->
```
insert into transaction values ('toy', 100.0, 4);
```
```
Constraint check c_gold_membership failed.
```

Finally, this fails since Adam does not have a membership at all.
<!-- ERROR -->
```
insert into transaction values ('toy', 100.0, 1);
```
```
Constraint check c_has_membership failed.
```

