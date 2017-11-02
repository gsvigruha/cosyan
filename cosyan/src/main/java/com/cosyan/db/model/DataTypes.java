package com.cosyan.db.model;

import java.util.Date;

import lombok.Data;

public class DataTypes {

  @Data
  public static class DataType<T> {
    private final String name;

    @Override
    public String toString() {
      return name;
    }
  }

  public static final DataType<String> StringType = new DataType<>("varchar");

  public static final DataType<Double> DoubleType = new DataType<>("float");

  public static final DataType<Long> LongType = new DataType<>("integer");

  public static final DataType<Boolean> BoolType = new DataType<>("boolean");

  public static final DataType<Date> DateType = new DataType<>("timestamp");

  public static final DataType<String[]> StringListType = new DataType<>("varchar_list");

  public static final DataType<Double[]> DoubleListType = new DataType<>("float_list");

  public static final DataType<Long[]> LongListType = new DataType<>("integer_list");

  public static final DataType<Boolean[]> BoolListType = new DataType<>("boolean_list");

  public static final DataType<Date[]> DateListType = new DataType<>("timestamp_list");

  public static final class NullType implements Comparable<Object> {
    @Override
    public boolean equals(Object o) {
      return o == NULL;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public int compareTo(Object o) {
      return o == NULL ? 0 : -1;
    }

    @Override
    public String toString() {
      return "null";
    }
  }

  public static final NullType NULL = new NullType();

  public static DataType<?> fromString(String name) {
    if (name.equals("varchar")) {
      return StringType;
    } else if (name.equals("float")) {
      return DoubleType;
    } else if (name.equals("integer")) {
      return LongType;
    } else if (name.equals("boolean")) {
      return BoolType;
    } else if (name.equals("timestamp")) {
      return DateType;
    } else if (name.equals("varchar_list")) {
      return StringListType;
    } else if (name.equals("float_list")) {
      return DoubleListType;
    } else if (name.equals("integer_list")) {
      return LongListType;
    } else if (name.equals("boolean_list")) {
      return BoolListType;
    } else if (name.equals("timestamp_list")) {
      return DateListType;
    } else {
      throw new IllegalArgumentException();
    }
  }
}
