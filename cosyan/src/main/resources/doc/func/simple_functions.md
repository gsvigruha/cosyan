 - `length(varchar): integer`
  Number of characters in self.
 - `upper(varchar): varchar`
  Converts all characters of self to uppercase.
 - `lower(varchar): varchar`
  Converts all characters of self to lowercase.
 - `substr(varchar, integer, integer): varchar`
  Returns the substring of self between startIndex and endIndex.
 - `matches(varchar, varchar): boolean`
  Returns true iff self matches the regular expression regex.
 - `contains(varchar, varchar): boolean`
  Returns true iff self contains str.
 - `replace(varchar, varchar, varchar): varchar`
  Replaces every occurrences of target with replacement in self.
 - `trim(varchar): varchar`
  Removes all leading and trailing whitespaces from self.
 - `concat(varchar, varchar): varchar`
  Concatenates self with str.
 - `index_of(varchar, varchar): integer`
  Index of the first occurrence of str in self.
 - `last_index_of(varchar, varchar): integer`
  Index of the last occurrence of str in self.
 - `pow(float, float): float`
  Returns the self raised to the power of exponent.
 - `exp(float): float`
  Returns Euler's number raised to the power of self.
 - `log(float, float): float`
  Returns the base self logarithm of x.
 - `log2(float): float`
  Returns the base 2 logarithm of self.
 - `loge(float): float`
  Returns the base e logarithm of self.
 - `log10(float): float`
  Returns the base 10 logarithm of self.
 - `round(float): integer`
  Rounds self to the nearest integer number.
 - `round_to(float, integer): float`
  Rounds self to d digits.
 - `ceil(float): integer`
  Returns the closest integer larger than self.
 - `floor(float): integer`
  Returns the closest integer smaller than self.
 - `abs(float): float`
  The absolute value of self.
 - `sin(float): float`
  Sine of self.
 - `sinh(float): float`
  Hyperbolic sine of self.
 - `cos(float): float`
  Cosine of self.
 - `cosh(float): float`
  Hyperbolic cosine of self.
 - `tan(float): float`
  Tangent of self.
 - `tanh(float): float`
  Hyperbolic tangent of self.
 - `date(varchar): timestamp`
  Converts self to a date.
 - `add_years(timestamp, integer): timestamp`
  Adds n years to self.
 - `add_months(timestamp, integer): timestamp`
  Adds n months to self.
 - `add_weeks(timestamp, integer): timestamp`
  Adds n weeks to self.
 - `add_days(timestamp, integer): timestamp`
  Adds n days to self.
 - `add_hours(timestamp, integer): timestamp`
  Adds n hours to self.
 - `add_seconds(timestamp, integer): timestamp`
  Adds n seconds to self.
 - `add_minutes(timestamp, integer): timestamp`
  Adds n minutes to self.
 - `get_year(timestamp): integer`
  Returns the year of self.
 - `get_month(timestamp): integer`
  Returns the month of self.
 - `get_week_of_year(timestamp): integer`
  Returns the week of year of self.
 - `get_week_of_month(timestamp): integer`
  Returns the week of month of self.
 - `get_day(timestamp): integer`
  Returns the day of month of self.
 - `get_day_of_year(timestamp): integer`
  Returns the day of year of self.
 - `get_day_of_month(timestamp): integer`
  Returns the day of month of self.
 - `get_day_of_week(timestamp): integer`
  Returns the day of week of self.
 - `get_hour(timestamp): integer`
  Returns the hours of self.
 - `get_minute(timestamp): integer`
  Returns the minutes of self.
 - `get_second(timestamp): integer`
  Returns the seconds of self.
