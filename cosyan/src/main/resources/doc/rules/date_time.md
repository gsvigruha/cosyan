# Date and time functions

Let's create a table with dates.
<!-- RUN -->
```
create table dates (d timestamp);
```

Add some data.
<!-- RUN -->
```
insert into dates values(dt '2017-01-01'), (dt '2017-01-01 10:30:00'), (dt '1960-01-01');
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

Malformatted dates result in `null`s.

<!-- TEST -->
```
select date('20170102') - d as diff from dates where d >= date('2017-01-01');
```
```
diff
null
null
```

<!-- TEST -->
```
select
  get_year(d) as d1,
  get_month(d) as d2,
  get_week_of_year(d) as d3,
  get_week_of_month(d) as d4,
  get_day(d) as d5,
  get_day_of_year(d) as d6,
  get_day_of_month(d) as d7,
  get_day_of_week(d) as d8,
  get_hour(d) as d9,
  get_minute(d) as d10,
  get_second(d) as d11
from dates;
```
```
d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11
2017,1,1,1,1,1,1,1,0,0,0
2017,1,1,1,1,1,1,1,10,30,0
1960,1,1,1,1,1,1,6,0,0,0
```
