# Date and time functions

Let's create a table with dates.
<!-- RUN -->
```
create table dates (d timestamp);
```

Add some data.
<!-- RUN -->
```
insert into dates values('2017-01-01'), ('2017-01-01 10:30:00'), ('1960-01-01');
```

<!-- TEST -->
```
select * from dates;
```
```
d
2017-01-01 00:00:00
2017-01-01 10:30:00
1960-01-01 00:00:00
```

Add a specific time interval to dates.

<!-- TEST -->
```
select
  add_years(d, 1) as d1,
  add_months(d, 1) as d2,
  add_weeks(d, 1) as d3,
  add_days(d, -1) as d4,
  add_hours(d, 1) as d5,
  add_minutes(d, 1) as d6,
  add_seconds(d, 1) as d7
from dates where d = date('2017-01-01');
```
```
d1,d2,d3,d4,d5,d6,d7
2018-01-01 00:00:00,2017-02-01 00:00:00,2017-01-08 00:00:00,2016-12-31 00:00:00,2017-01-01 01:00:00,2017-01-01 00:01:00,2017-01-01 00:00:01
```

<!-- TEST -->
```
select date('2017-01-02') - d as diff from dates where d >= date('2017-01-01');
```
```
diff
86400
48600
```




