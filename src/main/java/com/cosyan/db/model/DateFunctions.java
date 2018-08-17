/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.doc.FunctionDocumentation.FuncCat;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@FuncCat(name = "date", doc = "Date functions")
public class DateFunctions {

  public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  public static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

  public static java.util.Date convert(Object arg) {
    if (arg == null) {
      return null;
    }
    String sarg = (String) arg;
    try {
      return sdf1.parse(sarg);
    } catch (ParseException e1) {
      try {
        return sdf2.parse(sarg);
      } catch (ParseException e2) {
        return null;
      }
    }
  }

  @Func(doc = "Converts self to a date.")
  public static class Date extends SimpleFunction<java.util.Date> {

    public Date() {
      super("date", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.StringType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      String arg = (String) argValues.get(0);
      return convert(arg);
    }
  }

  private static Object add(ImmutableList<Object> argValues, int unit) {
    Calendar cal = Calendar.getInstance();
    cal.setTime((java.util.Date) argValues.get(0));
    cal.add(unit, ((Long) argValues.get(1)).intValue());
    return cal.getTime();
  }

  private static long get(ImmutableList<Object> argValues, int unit) {
    Calendar cal = Calendar.getInstance();
    cal.setTime((java.util.Date) argValues.get(0));
    return (long) cal.get(unit);
  }

  @Func(doc = "Adds n years to self.")
  public static class AddYears extends SimpleFunction<java.util.Date> {

    public AddYears() {
      super("add_years", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.YEAR);
    }
  }

  @Func(doc = "Adds n months to self.")
  public static class AddMonths extends SimpleFunction<java.util.Date> {

    public AddMonths() {
      super("add_months", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.MONTH);
    }
  }

  @Func(doc = "Adds n days to self.")
  public static class AddDays extends SimpleFunction<java.util.Date> {

    public AddDays() {
      super("add_days", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.DAY_OF_YEAR);
    }
  }

  @Func(doc = "Adds n weeks to self.")
  public static class AddWeeks extends SimpleFunction<java.util.Date> {

    public AddWeeks() {
      super("add_weeks", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.WEEK_OF_YEAR);
    }
  }

  @Func(doc = "Adds n hours to self.")
  public static class AddHours extends SimpleFunction<java.util.Date> {

    public AddHours() {
      super("add_hours", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.HOUR);
    }
  }

  @Func(doc = "Adds n minutes to self.")
  public static class AddMinutes extends SimpleFunction<java.util.Date> {

    public AddMinutes() {
      super("add_minutes", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.MINUTE);
    }
  }

  @Func(doc = "Adds n seconds to self.")
  public static class AddSeconds extends SimpleFunction<java.util.Date> {

    public AddSeconds() {
      super("add_seconds", DataTypes.dateType(), ImmutableMap.of("self", DataTypes.dateType(), "n", DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.SECOND);
    }
  }

  @Func(doc = "Returns the year of self.")
  public static class GetYear extends SimpleFunction<Long> {

    public GetYear() {
      super("get_year", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.YEAR);
    }
  }

  @Func(doc = "Returns the month of self.")
  public static class GetMonth extends SimpleFunction<Long> {

    public GetMonth() {
      super("get_month", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.MONTH) + 1;
    }
  }

  @Func(doc = "Returns the week of year of self.")
  public static class GetWeekOfYear extends SimpleFunction<Long> {

    public GetWeekOfYear() {
      super("get_week_of_year", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.WEEK_OF_YEAR);
    }
  }

  @Func(doc = "Returns the week of month of self.")
  public static class GetWeekOfMonth extends SimpleFunction<Long> {

    public GetWeekOfMonth() {
      super("get_week_of_month", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.WEEK_OF_MONTH);
    }
  }

  @Func(doc = "Returns the day of month of self.")
  public static class GetDay extends SimpleFunction<Long> {

    public GetDay() {
      super("get_day", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_MONTH);
    }
  }

  @Func(doc = "Returns the day of year of self.")
  public static class GetDayOfYear extends SimpleFunction<Long> {

    public GetDayOfYear() {
      super("get_day_of_year", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_YEAR);
    }
  }

  @Func(doc = "Returns the day of month of self.")
  public static class GetDayOfMonth extends SimpleFunction<Long> {

    public GetDayOfMonth() {
      super("get_day_of_month", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_MONTH);
    }
  }

  @Func(doc = "Returns the day of week of self.")
  public static class GetDayOfWeek extends SimpleFunction<Long> {

    public GetDayOfWeek() {
      super("get_day_of_week", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_WEEK);
    }
  }

  @Func(doc = "Returns the hours of self.")
  public static class GetHour extends SimpleFunction<Long> {

    public GetHour() {
      super("get_hour", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.HOUR);
    }
  }

  @Func(doc = "Returns the minutes of self.")
  public static class GetMinute extends SimpleFunction<Long> {

    public GetMinute() {
      super("get_minute", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.MINUTE);
    }
  }

  @Func(doc = "Returns the seconds of self.")
  public static class GetSecond extends SimpleFunction<Long> {

    public GetSecond() {
      super("get_second", DataTypes.LongType, ImmutableMap.of("self", DataTypes.dateType()));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.SECOND);
    }
  }
}
