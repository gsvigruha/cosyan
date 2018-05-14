## String functions.

 * `concat(self: varchar, str: varchar): varchar`<br/>
   Concatenates `self` with `str`.

 * `contains(self: varchar, str: varchar): boolean`<br/>
   Returns true iff `self` contains `str`.

 * `index_of(self: varchar, str: varchar): integer`<br/>
   Index of the first occurrence of `str` in `self`.

 * `last_index_of(self: varchar, str: varchar): integer`<br/>
   Index of the last occurrence of `str` in `self`.

 * `length(self: varchar): integer`<br/>
   Number of characters in `self`.

 * `lower(self: varchar): varchar`<br/>
   Converts all characters of `self` to lowercase.

 * `matches(self: varchar, regex: varchar): boolean`<br/>
   Returns true iff `self` matches the regular expression `regex`.

 * `replace(self: varchar, target: varchar, replacement: varchar): varchar`<br/>
   Replaces every occurrences of `target` with `replacement` in `self`.

 * `substr(self: varchar, start: integer, end: integer): varchar`<br/>
   Returns the substring of `self` between `start` and `end`.

 * `trim(self: varchar): varchar`<br/>
   Removes all leading and trailing whitespaces from `self`.

 * `upper(self: varchar): varchar`<br/>
   Converts all characters of `self` to uppercase.

## Mathematical functions.

 * `abs(self: float): float`<br/>
   The absolute value of `self`.

 * `ceil(self: float): integer`<br/>
   Returns the closest integer larger than `self`.

 * `cos(self: float): float`<br/>
   Cosine of `self`.

 * `cosh(self: float): float`<br/>
   Hyperbolic cosine of `self`.

 * `exp(self: float): float`<br/>
   Returns Euler's number raised to the power of `self`.

 * `floor(self: float): integer`<br/>
   Returns the closest integer smaller than `self`.

 * `log(self: float, x: float): float`<br/>
   Returns the base `self` logarithm of `x`.

 * `log10(self: float): float`<br/>
   Returns the base 10 logarithm of `self`.

 * `log2(self: float): float`<br/>
   Returns the base 2 logarithm of `self`.

 * `loge(self: float): float`<br/>
   Returns the base e logarithm of `self`.

 * `pow(self: float, x: float): float`<br/>
   Returns `self` raised to the power of `x`.

 * `round(self: float): integer`<br/>
   Rounds `self` to the nearest integer number.

 * `round_to(self: float, d: integer): float`<br/>
   Rounds `self` to `d` digits.

 * `sin(self: float): float`<br/>
   Sine of `self`.

 * `sinh(self: float): float`<br/>
   Hyperbolic sine of `self`.

 * `tan(self: float): float`<br/>
   Tangent of `self`.

 * `tanh(self: float): float`<br/>
   Hyperbolic tangent of `self`.

## Date functions.

 * `add_days(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` days to `self`.

 * `add_hours(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` hours to `self`.

 * `add_minutes(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` minutes to `self`.

 * `add_months(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` months to `self`.

 * `add_seconds(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` seconds to `self`.

 * `add_weeks(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` weeks to `self`.

 * `add_years(self: timestamp, n: integer): timestamp`<br/>
   Adds `n` years to `self`.

 * `date(self: varchar): timestamp`<br/>
   Converts `self` to a date.

 * `get_day(self: timestamp): integer`<br/>
   Returns the day of month of `self`.

 * `get_day_of_month(self: timestamp): integer`<br/>
   Returns the day of month of `self`.

 * `get_day_of_week(self: timestamp): integer`<br/>
   Returns the day of week of `self`.

 * `get_day_of_year(self: timestamp): integer`<br/>
   Returns the day of year of `self`.

 * `get_hour(self: timestamp): integer`<br/>
   Returns the hours of `self`.

 * `get_minute(self: timestamp): integer`<br/>
   Returns the minutes of `self`.

 * `get_month(self: timestamp): integer`<br/>
   Returns the month of `self`.

 * `get_second(self: timestamp): integer`<br/>
   Returns the seconds of `self`.

 * `get_week_of_month(self: timestamp): integer`<br/>
   Returns the week of month of `self`.

 * `get_week_of_year(self: timestamp): integer`<br/>
   Returns the week of year of `self`.

 * `get_year(self: timestamp): integer`<br/>
   Returns the year of `self`.

