package com.cosyan.db.model;

import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class BuiltinFunctions {

  @Data
  public static abstract class Function<T> {

    private final String ident;

    private final boolean isAggregation;

    private final DataType<T> returnType;

    private final ImmutableList<DataType<?>> argTypes;

    public abstract T call(ImmutableList<Object> argValues);
  }

  public static class Length extends Function<Long> {
    public Length() {
      super("length", false, DataTypes.LongType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) ((String) argValues.get(0)).length();
    }
  }

  public static class Upper extends Function<String> {
    public Upper() {
      super("upper", false, DataTypes.StringType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toUpperCase();
    }
  }

  public static class Lower extends Function<String> {
    public Lower() {
      super("lower", false, DataTypes.StringType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toLowerCase();
    }
  }

  public static class Substr extends Function<String> {
    public Substr() {
      super("substr", false, DataTypes.StringType,
          ImmutableList.of(DataTypes.StringType, DataTypes.LongType, DataTypes.LongType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      int start = ((Long) argValues.get(1)).intValue();
      int end = start + ((Long) argValues.get(2)).intValue();
      return ((String) argValues.get(0)).substring(start, end);
    }
  }

  public static final ImmutableList<Function<?>> ALL = ImmutableList.<Function<?>>builder()
      .add(new Length())
      .add(new Upper())
      .add(new Lower())
      .add(new Substr())
      .build();
}
