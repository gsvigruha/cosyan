package com.cosyan.db.model;

import java.util.Spliterator;

import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MathFunctions.Ceil;
import com.cosyan.db.model.MathFunctions.Exp;
import com.cosyan.db.model.MathFunctions.Floor;
import com.cosyan.db.model.MathFunctions.Log;
import com.cosyan.db.model.MathFunctions.Log10;
import com.cosyan.db.model.MathFunctions.Log2;
import com.cosyan.db.model.MathFunctions.LogE;
import com.cosyan.db.model.MathFunctions.Power;
import com.cosyan.db.model.MathFunctions.Round;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.StringFunctions.Contains;
import com.cosyan.db.model.StringFunctions.Length;
import com.cosyan.db.model.StringFunctions.Lower;
import com.cosyan.db.model.StringFunctions.Matches;
import com.cosyan.db.model.StringFunctions.Replace;
import com.cosyan.db.model.StringFunctions.Substr;
import com.cosyan.db.model.StringFunctions.Upper;
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

    public abstract Aggregator<T, ?> create();
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class AggrFunction extends Function {

    protected static final int characteristics = Spliterator.IMMUTABLE;

    public AggrFunction(String ident) {
      super(ident, false);
    }

    public abstract TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException;
  }

  public static final ImmutableList<AggrFunction> AGGREGATIONS = ImmutableList.<AggrFunction>builder()
      .add(new Aggregators.Sum())
      .add(new Aggregators.Count())
      .add(new Aggregators.CountDistinct())
      .add(new Aggregators.Max())
      .add(new Aggregators.Min())
      .build();

  public static final ImmutableList<SimpleFunction<?>> SIMPLE = ImmutableList.<SimpleFunction<?>>builder()
      .add(new Length())
      .add(new Upper())
      .add(new Lower())
      .add(new Substr())
      .add(new Matches())
      .add(new Contains())
      .add(new Replace())
      // Math
      .add(new Power())
      .add(new Exp())
      .add(new Log())
      .add(new Log2())
      .add(new LogE())
      .add(new Log10())
      .add(new Round())
      .add(new Ceil())
      .add(new Floor())
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
