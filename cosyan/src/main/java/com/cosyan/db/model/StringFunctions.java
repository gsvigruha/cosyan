package com.cosyan.db.model;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StringFunctions {
  @Func(doc = "Number of characters in self.")
  public static class Length extends SimpleFunction<Long> {
    public Length() {
      super("length", DataTypes.LongType, ImmutableMap.of("self", DataTypes.StringType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) ((String) argValues.get(0)).length();
    }
  }

  @Func(doc = "Converts all characters of self to uppercase.")
  public static class Upper extends SimpleFunction<String> {
    public Upper() {
      super("upper", DataTypes.StringType, ImmutableMap.of("self", DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toUpperCase();
    }
  }

  @Func(doc = "Converts all characters of self to lowercase.")
  public static class Lower extends SimpleFunction<String> {
    public Lower() {
      super("lower", DataTypes.StringType, ImmutableMap.of("self", DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      return ((String) argValues.get(0)).toLowerCase();
    }
  }

  @Func(doc = "Returns the substring of self between start and end.")
  public static class Substr extends SimpleFunction<String> {
    public Substr() {
      super("substr", DataTypes.StringType,
          ImmutableMap.of("self", DataTypes.StringType, "start", DataTypes.LongType, "end", DataTypes.LongType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      int start = ((Long) argValues.get(1)).intValue();
      int end = start + ((Long) argValues.get(2)).intValue();
      return ((String) argValues.get(0)).substring(start, end);
    }
  }

  @Func(doc = "Returns true iff self matches the regular expression regex.")
  public static class Matches extends SimpleFunction<Boolean> {
    public Matches() {
      super("matches", DataTypes.BoolType,
          ImmutableMap.of("self", DataTypes.StringType, "regex", DataTypes.StringType));
    }

    @Override
    public Boolean call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String regex = (String) argValues.get(1);
      return str.matches(regex);
    }
  }

  @Func(doc = "Returns true iff self contains str.")
  public static class Contains extends SimpleFunction<Boolean> {
    public Contains() {
      super("contains", DataTypes.BoolType,
          ImmutableMap.of("self", DataTypes.StringType, "str", DataTypes.StringType));
    }

    @Override
    public Boolean call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String substring = (String) argValues.get(1);
      return str.contains(substring);
    }
  }

  @Func(doc = "Replaces every occurrences of target with replacement in self.")
  public static class Replace extends SimpleFunction<String> {
    public Replace() {
      super("replace", DataTypes.StringType, ImmutableMap.of(
          "self", DataTypes.StringType,
          "target", DataTypes.StringType,
          "replacement", DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String oldStr = (String) argValues.get(1);
      String newStr = (String) argValues.get(2);
      return str.replace(oldStr, newStr);
    }
  }

  @Func(doc = "Removes all leading and trailing whitespaces from self.")
  public static class Trim extends SimpleFunction<String> {
    public Trim() {
      super("trim", DataTypes.StringType, ImmutableMap.of("self", DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      return str.trim();
    }
  }

  @Func(doc = "Concatenates self with str.")
  public static class Concat extends SimpleFunction<String> {
    public Concat() {
      super("concat", DataTypes.StringType, ImmutableMap.of("self", DataTypes.StringType, "str", DataTypes.StringType));
    }

    @Override
    public String call(ImmutableList<Object> argValues) {
      String str1 = (String) argValues.get(0);
      String str2 = (String) argValues.get(1);
      return str1.concat(str2);
    }
  }

  @Func(doc = "Index of the first occurrence of str in self.")
  public static class IndexOf extends SimpleFunction<Long> {
    public IndexOf() {
      super("index_of", DataTypes.LongType, ImmutableMap.of("self", DataTypes.StringType, "str", DataTypes.StringType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String substr = (String) argValues.get(1);
      return (long) str.indexOf(substr);
    }
  }

  @Func(doc = "Index of the last occurrence of str in self.")
  public static class LastIndexOf extends SimpleFunction<Long> {
    public LastIndexOf() {
      super("last_index_of", DataTypes.LongType,
          ImmutableMap.of("self", DataTypes.StringType, "str", DataTypes.StringType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      String str = (String) argValues.get(0);
      String substr = (String) argValues.get(1);
      return (long) str.lastIndexOf(substr);
    }
  }
}
