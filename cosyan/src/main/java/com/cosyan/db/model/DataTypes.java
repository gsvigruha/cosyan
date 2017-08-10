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

  public static final DataType<String> StringType = new DataType<>("String");

  public static final DataType<Double> DoubleType = new DataType<>("Double");

  public static final DataType<Long> LongType = new DataType<>("Long");

  public static final DataType<Boolean> BoolType = new DataType<>("Boolean");

  public static final DataType<Date> DateType = new DataType<>("Date");
}