package com.cosyan.db.model;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.doc.FunctionDocumentation.FuncCat;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@FuncCat(name = "math", doc = "Arithmetic functions")
public class MathFunctions {

  @Func(doc = "Returns self raised to the power of x.")
  public static class Power extends SimpleFunction<Double> {
    public Power() {
      super("pow", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType, "x", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.pow((Double) argValues.get(0), (Double) argValues.get(1));
    }
  }

  @Func(doc = "Returns Euler's number raised to the power of self.")
  public static class Exp extends SimpleFunction<Double> {
    public Exp() {
      super("exp", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.exp((Double) argValues.get(0));
    }
  }

  @Func(doc = "Returns the base 2 logarithm of self.")
  public static class Log2 extends SimpleFunction<Double> {
    public Log2() {
      super("log2", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0)) / Math.log(2);
    }
  }

  @Func(doc = "Returns the base e logarithm of self.")
  public static class LogE extends SimpleFunction<Double> {
    public LogE() {
      super("loge", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0));
    }
  }

  @Func(doc = "Returns the base 10 logarithm of self.")
  public static class Log10 extends SimpleFunction<Double> {
    public Log10() {
      super("log10", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log10((Double) argValues.get(0));
    }
  }

  @Func(doc = "Returns the base self logarithm of x.")
  public static class Log extends SimpleFunction<Double> {
    public Log() {
      super("log", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType, "x", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0)) / Math.log((Double) argValues.get(1));
    }
  }

  @Func(doc = "Rounds self to the nearest integer number.")
  public static class Round extends SimpleFunction<Long> {
    public Round() {
      super("round", DataTypes.LongType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return Math.round((Double) argValues.get(0));
    }
  }

  @Func(doc = "Rounds self to d digits.")
  public static class RoundTo extends SimpleFunction<Double> {
    public RoundTo() {
      super("round_to", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType, "d", DataTypes.LongType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      double exp = Math.pow(10, (Long) argValues.get(1));
      return Math.round((Double) argValues.get(0) * exp) / exp;
    }
  }

  @Func(doc = "Returns the closest integer larger than self.")
  public static class Ceil extends SimpleFunction<Long> {
    public Ceil() {
      super("ceil", DataTypes.LongType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) Math.ceil((Double) argValues.get(0));
    }
  }

  @Func(doc = "Returns the closest integer smaller than self.")
  public static class Floor extends SimpleFunction<Long> {
    public Floor() {
      super("floor", DataTypes.LongType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) Math.floor((Double) argValues.get(0));
    }
  }

  @Func(doc = "The absolute value of self.")
  public static class Abs extends SimpleFunction<Double> {
    public Abs() {
      super("abs", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.abs((Double) argValues.get(0));
    }
  }

  @Func(doc = "Sine of self.")
  public static class Sin extends SimpleFunction<Double> {
    public Sin() {
      super("sin", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.sin((Double) argValues.get(0));
    }
  }

  @Func(doc = "Hyperbolic sine of self.")
  public static class SinH extends SimpleFunction<Double> {
    public SinH() {
      super("sinh", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.sinh((Double) argValues.get(0));
    }
  }

  @Func(doc = "Cosine of self.")
  public static class Cos extends SimpleFunction<Double> {
    public Cos() {
      super("cos", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.cos((Double) argValues.get(0));
    }
  }

  @Func(doc = "Hyperbolic cosine of self.")
  public static class CosH extends SimpleFunction<Double> {
    public CosH() {
      super("cosh", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.cosh((Double) argValues.get(0));
    }
  }

  @Func(doc = "Tangent of self.")
  public static class Tan extends SimpleFunction<Double> {
    public Tan() {
      super("tan", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.tan((Double) argValues.get(0));
    }
  }

  @Func(doc = "Hyperbolic tangent of self.")
  public static class TanH extends SimpleFunction<Double> {
    public TanH() {
      super("tanh", DataTypes.DoubleType, ImmutableMap.of("self", DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.tanh((Double) argValues.get(0));
    }
  }
}
