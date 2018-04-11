package com.cosyan.db.model;

import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.google.common.collect.ImmutableList;

public class MathFunctions {

  public static class Power extends SimpleFunction<Double> {
    public Power() {
      super("pow", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType, DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.pow((Double) argValues.get(0), (Double) argValues.get(1));
    }
  }

  public static class Exp extends SimpleFunction<Double> {
    public Exp() {
      super("exp", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.exp((Double) argValues.get(0));
    }
  }

  public static class Log2 extends SimpleFunction<Double> {
    public Log2() {
      super("log2", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0)) / Math.log(2);
    }
  }

  public static class LogE extends SimpleFunction<Double> {
    public LogE() {
      super("loge", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0));
    }
  }

  public static class Log10 extends SimpleFunction<Double> {
    public Log10() {
      super("log10", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log10((Double) argValues.get(0));
    }
  }

  public static class Log extends SimpleFunction<Double> {
    public Log() {
      super("log", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType, DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.log((Double) argValues.get(0)) / Math.log((Double) argValues.get(1));
    }
  }

  public static class Round extends SimpleFunction<Long> {
    public Round() {
      super("round", DataTypes.LongType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return Math.round((Double) argValues.get(0));
    }
  }

  public static class RoundTo extends SimpleFunction<Double> {
    public RoundTo() {
      super("round_to", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType, DataTypes.LongType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      double exp = Math.pow(10, (Long) argValues.get(1));
      return Math.round((Double) argValues.get(0) * exp) / exp;
    }
  }

  public static class Ceil extends SimpleFunction<Long> {
    public Ceil() {
      super("ceil", DataTypes.LongType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) Math.ceil((Double) argValues.get(0));
    }
  }

  public static class Floor extends SimpleFunction<Long> {
    public Floor() {
      super("floor", DataTypes.LongType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Long call(ImmutableList<Object> argValues) {
      return (long) Math.floor((Double) argValues.get(0));
    }
  }

  public static class Abs extends SimpleFunction<Double> {
    public Abs() {
      super("abs", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.abs((Double) argValues.get(0));
    }
  }

  public static class Sin extends SimpleFunction<Double> {
    public Sin() {
      super("sin", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.sin((Double) argValues.get(0));
    }
  }

  public static class SinH extends SimpleFunction<Double> {
    public SinH() {
      super("sinh", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.sinh((Double) argValues.get(0));
    }
  }

  public static class Cos extends SimpleFunction<Double> {
    public Cos() {
      super("cos", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.cos((Double) argValues.get(0));
    }
  }

  public static class CosH extends SimpleFunction<Double> {
    public CosH() {
      super("cosh", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.cosh((Double) argValues.get(0));
    }
  }

  public static class Tan extends SimpleFunction<Double> {
    public Tan() {
      super("tan", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.tan((Double) argValues.get(0));
    }
  }

  public static class TanH extends SimpleFunction<Double> {
    public TanH() {
      super("tanh", DataTypes.DoubleType, ImmutableList.of(DataTypes.DoubleType));
    }

    @Override
    public Double call(ImmutableList<Object> argValues) {
      return Math.tanh((Double) argValues.get(0));
    }
  }
}
