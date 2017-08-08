package com.cosyan.db.model;

import java.util.Date;

public class DataTypes {

  public static class DataType<T> {

  }

  public static final DataType<String> StringType = new DataType<>();

  public static final DataType<Double> DoubleType = new DataType<>();

  public static final DataType<Long> LongType = new DataType<>();

  public static final DataType<Boolean> BoolType = new DataType<>();

  public static final DataType<Date> DateType = new DataType<>();
}
