package com.cosyan.db.model;

import java.util.Spliterator;

import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class BuiltinFunctions {

  @Data
  public static abstract class Function {

    protected final String ident;

    private final boolean isAggregation;
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class SimpleFunction<T> extends Function {

    private final ImmutableList<DataType<?>> argTypes;

    private final DataType<T> returnType;

    public SimpleFunction(String ident, DataType<T> returnType, ImmutableList<DataType<?>> argTypes) {
      super(ident, false);
      this.argTypes = argTypes;
      this.returnType = returnType;
    }

    public abstract T call(ImmutableList<Object> argValues);
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class TypedAggrFunction<T> extends Function {

    private final DataType<T> returnType;

    public TypedAggrFunction(String ident, DataType<T> returnType) {
      super(ident, false);
      this.returnType = returnType;
    }

    public abstract Object init();

    public abstract Object aggregate(Object a, Object x);
    
    public abstract T finish(Object x);
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class AggrFunction extends Function {

    protected static final int characteristics = Spliterator.IMMUTABLE;

    public AggrFunction(String ident) {
      super(ident, false);
    }

    public abstract TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException;
  }

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

  public static final ImmutableList<AggrFunction> AGGREGATIONS = ImmutableList.<AggrFunction>builder()
      .add(new Aggregators.Sum())
      .add(new Aggregators.Count())
      .add(new Aggregators.Max())
      .add(new Aggregators.Min())
      .build();

  public static final ImmutableList<SimpleFunction<?>> SIMPLE = ImmutableList.<SimpleFunction<?>>builder()
      .add(new Length())
      .add(new Upper())
      .add(new Lower())
      .add(new Substr())
      .build();

  public static final ImmutableSet<String> AGGREGATION_NAMES;
  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (AggrFunction function : AGGREGATIONS) {
      builder.add(function.getIdent());
    }
    AGGREGATION_NAMES = builder.build();
  }
}
