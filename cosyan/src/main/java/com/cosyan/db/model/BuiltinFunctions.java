package com.cosyan.db.model;

import java.util.Spliterator;

import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

    private final DataType<?> argType;

    private final DataType<T> returnType;

    public TypedAggrFunction(String ident, DataType<T> returnType, DataType<?> argType) {
      super(ident, false);
      this.argType = argType;
      this.returnType = returnType;
    }

    public abstract Object aggregate(Object x, Object y);
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

  public static class Sum extends AggrFunction {
    public Sum() {
      super("sum");
    }

    @Override
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType, DataTypes.DoubleType) {
          @Override
          public Double aggregate(Object x, Object y) {
            return (Double) x + (Double) y;
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType, DataTypes.LongType) {
          @Override
          public Long aggregate(Object x, Object y) {
            return (Long) x + (Long) y;
          }
        };
      } else {
        throw new ModelException("Invalid type for sum: '" + argType + "'.");
      }
    }
  }

  public static final ImmutableMap<String, AggrFunction> AGGREGATIONS = ImmutableMap.<String, AggrFunction>builder()
      .put("sum", new Sum())
      .build();

  public static final ImmutableList<SimpleFunction<?>> SIMPLE = ImmutableList.<SimpleFunction<?>>builder()
      .add(new Length())
      .add(new Upper())
      .add(new Lower())
      .add(new Substr())
      .build();

}
