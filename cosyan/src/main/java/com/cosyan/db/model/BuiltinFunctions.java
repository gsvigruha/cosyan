package com.cosyan.db.model;

import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MathFunctions.Ceil;
import com.cosyan.db.model.MathFunctions.Cos;
import com.cosyan.db.model.MathFunctions.Exp;
import com.cosyan.db.model.MathFunctions.Floor;
import com.cosyan.db.model.MathFunctions.Log;
import com.cosyan.db.model.MathFunctions.Log10;
import com.cosyan.db.model.MathFunctions.Log2;
import com.cosyan.db.model.MathFunctions.LogE;
import com.cosyan.db.model.MathFunctions.Power;
import com.cosyan.db.model.MathFunctions.Round;
import com.cosyan.db.model.MathFunctions.Sin;
import com.cosyan.db.model.StringFunctions.Contains;
import com.cosyan.db.model.StringFunctions.Length;
import com.cosyan.db.model.StringFunctions.Lower;
import com.cosyan.db.model.StringFunctions.Matches;
import com.cosyan.db.model.StringFunctions.Replace;
import com.cosyan.db.model.StringFunctions.Substr;
import com.cosyan.db.model.StringFunctions.Upper;
import com.cosyan.db.model.TableFunctions.SelectFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class TableFunction extends Function {

    public TableFunction(String ident) {
      super(ident, false);
    }

    public abstract TableMeta call(TableMeta tableMeta, ImmutableMap<String, ColumnMeta> argValues);
  }

  public static final ImmutableList<AggrFunction> AGGREGATIONS = ImmutableList.<AggrFunction>builder()
      .add(new Aggregators.Sum())
      .add(new Aggregators.Count())
      .add(new Aggregators.CountDistinct())
      .add(new Aggregators.Max())
      .add(new Aggregators.Min())
      .add(new ListAggregator())
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
      .add(new Sin())
      .add(new Cos())
      .build();

  public static final ImmutableList<TableFunction> TABLE = ImmutableList.<TableFunction>builder()
      .add(new SelectFunction("select"))
      .build();
      
  public static final ImmutableSet<String> AGGREGATION_NAMES;
  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (AggrFunction function : AGGREGATIONS) {
      builder.add(function.getIdent());
    }
    AGGREGATION_NAMES = builder.build();
  }

  private static final ConcurrentHashMap<String, SimpleFunction<?>> simpleFunctions;
  private static final ConcurrentHashMap<String, AggrFunction> aggrFunctions;
  private static final ConcurrentHashMap<String, TableFunction> tableFunctions;

  static {
    simpleFunctions = new ConcurrentHashMap<>();
    for (SimpleFunction<?> simpleFunction : BuiltinFunctions.SIMPLE) {
      simpleFunctions.put(simpleFunction.getIdent(), simpleFunction);
    }
    aggrFunctions = new ConcurrentHashMap<>();
    for (AggrFunction aggrFunction : BuiltinFunctions.AGGREGATIONS) {
      aggrFunctions.put(aggrFunction.getIdent(), aggrFunction);
    }
    tableFunctions = new ConcurrentHashMap<>();
    for (TableFunction tableFunction : BuiltinFunctions.TABLE) {
      tableFunctions.put(tableFunction.getIdent(), tableFunction);
    }
  }

  public static SimpleFunction<?> simpleFunction(String ident) throws ModelException {
    if (!simpleFunctions.containsKey(ident)) {
      throw new ModelException("Function " + ident + " does not exist.");
    }
    return simpleFunctions.get(ident);
  }

  public static TypedAggrFunction<?> aggrFunction(String ident, DataType<?> argType) throws ModelException {
    if (!aggrFunctions.containsKey(ident)) {
      throw new ModelException("Function " + ident + " does not exist.");
    }
    return aggrFunctions.get(ident).compile(argType);
  }
  
  public static TableFunction tableFunction(String ident) throws ModelException {
    if (!tableFunctions.containsKey(ident)) {
      throw new ModelException("Function " + ident + " does not exist.");
    }
    return tableFunctions.get(ident);
  }
}
