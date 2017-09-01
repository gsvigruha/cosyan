package com.cosyan.db.model;

import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;

public class StringFunctions {
  public static class Length extends SimpleFunction<Long> {
    public Length() {
      super("length", DataTypes.LongType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) ((String) argValues.get(0)).length();
    }
  }

  public static class Upper extends SimpleFunction<String> {
    public Upper() {
      super("upper", DataTypes.StringType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toUpperCase();
    }
  }

  public static class Lower extends SimpleFunction<String> {
    public Lower() {
      super("lower", DataTypes.StringType, ImmutableList.of(DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toLowerCase();
    }
  }

  public static class Substr extends SimpleFunction<String> {
    public Substr() {
      super("substr", DataTypes.StringType,
          ImmutableList.of(DataTypes.StringType, DataTypes.LongType, DataTypes.LongType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      int start = ((Long) argValues.get(1)).intValue();
      int end = start + ((Long) argValues.get(2)).intValue();
      return ((String) argValues.get(0)).substring(start, end);
    }
  }

  public static class Matches extends SimpleFunction<Boolean> {
    public Matches() {
      super("matches", DataTypes.BoolType,
          ImmutableList.of(DataTypes.StringType, DataTypes.StringType));
    }

    @Override
    public Boolean call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String regex = (String) argValues.get(1);
      return str.matches(regex);
    }
  }

  public static class Contains extends SimpleFunction<Boolean> {
    public Contains() {
      super("contains", DataTypes.BoolType,
          ImmutableList.of(DataTypes.StringType, DataTypes.StringType));
    }

    @Override
    public Boolean call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String substring = (String) argValues.get(1);
      return str.contains(substring);
    }
  }

  public static class Replace extends SimpleFunction<String> {
    public Replace() {
      super("replace", DataTypes.StringType,
          ImmutableList.of(DataTypes.StringType, DataTypes.StringType, DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String oldStr = (String) argValues.get(1);
      String newStr = (String) argValues.get(2);
      return str.replace(oldStr, newStr);
    }
  }
}
