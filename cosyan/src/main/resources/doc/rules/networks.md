# Networks

<!-- RUN -->
```
create table people (
  id integer,
  name varchar,
  age integer,
  constraint pk_id primary key (id));
```

<!-- RUN -->
```
create table relationship (
  src_id integer not null,
  dst_id integer not null,
  weight float,
  constraint src foreign key (src_id) references people(id) reverse og,
  constraint dst foreign key (dst_id) references people(id) reverse ic);
```

<!-- RUN -->
```
alter table people add ref neighbors (
  select
    max(dst.age) as max_age,
    sum(dst.age * weight) / sum(weight) as avg_age
  from og
);
```
<!-- RUN -->
```
alter table people add ref neighbors_2 (
  select
    max(dst.neighbors.max_age) as max_age
  from og
);
```

<!-- RUN -->
```
insert into people values (1, 'Adam', 30), (2, 'Bob', 40), (3, 'Cecil', 20), (4, 'Dave', 50);
insert into relationship values (1, 2, 1.0), (2, 3, 2.0), (2, 4, 3.0);
```

<!-- TEST -->
```
select name, neighbors.max_age, neighbors.avg_age, neighbors_2.max_age as max_age_2 from people;
```
```
name,max_age,avg_age,max_age_2
Adam,40,40.0,50
Bob,50,38.0,null
Cecil,null,null,null
Dave,null,null,null
```