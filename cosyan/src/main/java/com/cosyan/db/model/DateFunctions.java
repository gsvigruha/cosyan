package com.cosyan.db.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;

public class DateFunctions {

  public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

  public static Object convert(Object arg) {
    if (arg == DataTypes.NULL) {
      return arg;
    }
    String sarg = (String) arg;
    try {
      return sdf1.parse(sarg);
    } catch (ParseException e1) {
      try {
        return sdf2.parse(sarg);
      } catch (ParseException e2) {
        return DataTypes.NULL;
      }
    }
  }

  public static class Date extends SimpleFunction<java.util.Date> {

    public Date() {
      super("date", DataTypes.DateType, ImmutableList.of(DataTypes.StringType));
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

  public static class AddYears extends SimpleFunction<java.util.Date> {

    public AddYears() {
      super("add_years", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.YEAR);
    }
  }

  public static class AddMonths extends SimpleFunction<java.util.Date> {

    public AddMonths() {
      super("add_months", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.MONTH);
    }
  }

  public static class AddDays extends SimpleFunction<java.util.Date> {

    public AddDays() {
      super("add_days", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.DAY_OF_YEAR);
    }
  }

  public static class AddWeeks extends SimpleFunction<java.util.Date> {

    public AddWeeks() {
      super("add_weeks", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.WEEK_OF_YEAR);
    }
  }

  public static class AddHours extends SimpleFunction<java.util.Date> {

    public AddHours() {
      super("add_hours", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.HOUR);
    }
  }

  public static class AddMinutes extends SimpleFunction<java.util.Date> {

    public AddMinutes() {
      super("add_minutes", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.MINUTE);
    }
  }

  public static class AddSeconds extends SimpleFunction<java.util.Date> {

    public AddSeconds() {
      super("add_seconds", DataTypes.DateType, ImmutableList.of(DataTypes.DateType, DataTypes.LongType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return add(argValues, Calendar.SECOND);
    }
  }

  public static class GetYear extends SimpleFunction<Long> {

    public GetYear() {
      super("get_year", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.YEAR);
    }
  }

  public static class GetMonth extends SimpleFunction<Long> {

    public GetMonth() {
      super("get_month", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.MONTH) + 1;
    }
  }

  public static class GetWeekOfYear extends SimpleFunction<Long> {

    public GetWeekOfYear() {
      super("get_week_of_year", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.WEEK_OF_YEAR);
    }
  }

  public static class GetWeekOfMonth extends SimpleFunction<Long> {

    public GetWeekOfMonth() {
      super("get_week_of_month", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.WEEK_OF_MONTH);
    }
  }

  public static class GetDay extends SimpleFunction<Long> {

    public GetDay() {
      super("get_day", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_MONTH);
    }
  }

  public static class GetDayOfYear extends SimpleFunction<Long> {

    public GetDayOfYear() {
      super("get_day_of_year", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_YEAR);
    }
  }

  public static class GetDayOfMonth extends SimpleFunction<Long> {

    public GetDayOfMonth() {
      super("get_day_of_month", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_MONTH);
    }
  }

  public static class GetDayOfWeek extends SimpleFunction<Long> {

    public GetDayOfWeek() {
      super("get_day_of_week", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.DAY_OF_WEEK);
    }
  }

  public static class GetHour extends SimpleFunction<Long> {

    public GetHour() {
      super("get_hour", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.HOUR);
    }
  }

  public static class GetMinute extends SimpleFunction<Long> {

    public GetMinute() {
      super("get_minute", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.MINUTE);
    }
  }

  public static class GetSecond extends SimpleFunction<Long> {

    public GetSecond() {
      super("get_second", DataTypes.LongType, ImmutableList.of(DataTypes.DateType));
    }

    @Override
    public Object call(ImmutableList<Object> argValues) {
      return get(argValues, Calendar.SECOND);
    }
  }
}
