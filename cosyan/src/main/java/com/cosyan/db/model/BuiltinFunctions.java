package com.cosyan.db.model;

import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.DateFunctions.AddDays;
import com.cosyan.db.model.DateFunctions.AddHours;
import com.cosyan.db.model.DateFunctions.AddMinutes;
import com.cosyan.db.model.DateFunctions.AddMonths;
import com.cosyan.db.model.DateFunctions.AddSeconds;
import com.cosyan.db.model.DateFunctions.AddWeeks;
import com.cosyan.db.model.DateFunctions.AddYears;
import com.cosyan.db.model.DateFunctions.Date;
import com.cosyan.db.model.DateFunctions.GetDay;
import com.cosyan.db.model.DateFunctions.GetDayOfMonth;
import com.cosyan.db.model.DateFunctions.GetDayOfWeek;
import com.cosyan.db.model.DateFunctions.GetDayOfYear;
import com.cosyan.db.model.DateFunctions.GetHour;
import com.cosyan.db.model.DateFunctions.GetMinute;
import com.cosyan.db.model.DateFunctions.GetMonth;
import com.cosyan.db.model.DateFunctions.GetSecond;
import com.cosyan.db.model.DateFunctions.GetWeekOfMonth;
import com.cosyan.db.model.DateFunctions.GetWeekOfYear;
import com.cosyan.db.model.DateFunctions.GetYear;
import com.cosyan.db.model.MathFunctions.Abs;
import com.cosyan.db.model.MathFunctions.Ceil;
import com.cosyan.db.model.MathFunctions.Cos;
import com.cosyan.db.model.MathFunctions.CosH;
import com.cosyan.db.model.MathFunctions.Exp;
import com.cosyan.db.model.MathFunctions.Floor;
import com.cosyan.db.model.MathFunctions.Log;
import com.cosyan.db.model.MathFunctions.Log10;
import com.cosyan.db.model.MathFunctions.Log2;
import com.cosyan.db.model.MathFunctions.LogE;
import com.cosyan.db.model.MathFunctions.Power;
import com.cosyan.db.model.MathFunctions.Round;
import com.cosyan.db.model.MathFunctions.RoundTo;
import com.cosyan.db.model.MathFunctions.Sin;
import com.cosyan.db.model.MathFunctions.SinH;
import com.cosyan.db.model.MathFunctions.Tan;
import com.cosyan.db.model.MathFunctions.TanH;
import com.cosyan.db.model.StringFunctions.Concat;
import com.cosyan.db.model.StringFunctions.Contains;
import com.cosyan.db.model.StringFunctions.IndexOf;
import com.cosyan.db.model.StringFunctions.LastIndexOf;
import com.cosyan.db.model.StringFunctions.Length;
import com.cosyan.db.model.StringFunctions.Lower;
import com.cosyan.db.model.StringFunctions.Matches;
import com.cosyan.db.model.StringFunctions.Replace;
import com.cosyan.db.model.StringFunctions.Substr;
import com.cosyan.db.model.StringFunctions.Trim;
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

    public abstract Object call(ImmutableList<Object> argValues);
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
      .add(new StatAggregators.Sum())
      .add(new Aggregators.Count())
      .add(new StatAggregators.Avg())
      .add(new StatAggregators.StdDev())
      .add(new StatAggregators.StdDevPop())
      .add(new StatAggregators.Skewness())
      .add(new StatAggregators.Kurtosis())
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
      .add(new Trim())
      .add(new Concat())
      .add(new IndexOf())
      .add(new LastIndexOf())
      // Math
      .add(new Power())
      .add(new Exp())
      .add(new Log())
      .add(new Log2())
      .add(new LogE())
      .add(new Log10())
      .add(new Round())
      .add(new RoundTo())
      .add(new Ceil())
      .add(new Floor())
      .add(new Abs())
      .add(new Sin())
      .add(new SinH())
      .add(new Cos())
      .add(new CosH())
      .add(new Tan())
      .add(new TanH())
      // Date
      .add(new Date())
      .add(new AddYears())
      .add(new AddMonths())
      .add(new AddWeeks())
      .add(new AddDays())
      .add(new AddHours())
      .add(new AddSeconds())
      .add(new AddMinutes())
      .add(new GetYear())
      .add(new GetMonth())
      .add(new GetWeekOfYear())
      .add(new GetWeekOfMonth())
      .add(new GetDay())
      .add(new GetDayOfYear())
      .add(new GetDayOfMonth())
      .add(new GetDayOfWeek())
      .add(new GetHour())
      .add(new GetMinute())
      .add(new GetSecond())
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
