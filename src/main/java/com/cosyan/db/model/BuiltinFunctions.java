/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.cosyan.db.model.ListAggregators.ListAggregator;
import com.cosyan.db.model.ListAggregators.SetAggregator;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class BuiltinFunctions {

  @Data
  public static abstract class Function {

    protected final String name;

    private final boolean isAggregation;
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class SimpleFunction<T> extends Function {

    private final ImmutableMap<String, DataType<?>> argTypes;

    private final DataType<T> returnType;

    public SimpleFunction(String ident, DataType<T> returnType, ImmutableMap<String, DataType<?>> argTypes) {
      super(ident, false);
      this.argTypes = argTypes;
      this.returnType = returnType;
    }

    public DataType<?> argType(int i) {
      return argTypes.values().asList().get(i);
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

    public abstract TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException;
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
      .add(new SetAggregator())
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

  public static final ImmutableSet<String> AGGREGATION_NAMES;
  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (AggrFunction function : AGGREGATIONS) {
      builder.add(function.getName());
    }
    AGGREGATION_NAMES = builder.build();
  }

  private static final ConcurrentHashMap<String, SimpleFunction<?>> simpleFunctions;
  private static final ConcurrentHashMap<String, AggrFunction> aggrFunctions;

  static {
    simpleFunctions = new ConcurrentHashMap<>();
    for (SimpleFunction<?> simpleFunction : BuiltinFunctions.SIMPLE) {
      simpleFunctions.put(simpleFunction.getName(), simpleFunction);
    }
    aggrFunctions = new ConcurrentHashMap<>();
    for (AggrFunction aggrFunction : BuiltinFunctions.AGGREGATIONS) {
      aggrFunctions.put(aggrFunction.getName(), aggrFunction);
    }
  }

  public static SimpleFunction<?> simpleFunction(Ident ident) throws ModelException {
    String name = ident.getString();
    if (!simpleFunctions.containsKey(name)) {
      throw new ModelException("Function " + name + " does not exist.", ident);
    }
    return simpleFunctions.get(name);
  }

  public static TypedAggrFunction<?> aggrFunction(Ident ident, DataType<?> argType) throws ModelException {
    String name = ident.getString();
    if (!aggrFunctions.containsKey(name)) {
      throw new ModelException("Function " + name + " does not exist.", ident);
    }
    return aggrFunctions.get(name).compile(ident, argType);
  }
}
