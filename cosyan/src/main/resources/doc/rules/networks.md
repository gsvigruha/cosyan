# Networks

<!-- RUN -->
```
create table people (
  id integer,
  name varchar,
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
alter table people add ref w (select sum(weight) as sw1 from og);
```
<!-- RUN -->
```
alter table people add ref w2 (select sum(dst.w.sw1) as sw2 from og);
```

<!-- RUN -->
```
insert into people values (1, 'Adam'), (2, 'Bob'), (3, 'Cecil'), (4, 'Dave');
insert into relationship values (1, 2, 1.0), (2, 3, 10.0), (2, 4, 100.0);
```

<!-- TEST -->
```
select name, w.sw1, w2.sw2 from people;
```
```
name,sw1,sw2
Adam,1.0,110.0
Bob,110.0,null
Cecil,null,null
Dave,null,null
```