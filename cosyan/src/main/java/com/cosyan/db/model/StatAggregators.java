package com.cosyan.db.model;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.doc.FunctionDocumentation.FuncCat;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.StatAggregators.Skewness.SkewnessAggregator;

@FuncCat(name = "stats", doc = "Statistical functions.")
public class StatAggregators {

  @Func(doc = "The sum of the elements.")
  public static class Sum extends AggrFunction {
    public Sum() {
      super("sum");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new Aggregator<Double, Double>() {

              private Double sum = null;

              @Override
              public void addImpl(Double x) {
                if (sum == null) {
                  sum = x;
                } else {
                  sum += x;
                }
              }

              @Override
              public Double finishImpl() {
                return sum;
              }

              @Override
              public boolean isNull() {
                return sum == null;
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Long>(name, DataTypes.LongType) {

          @Override
          public Aggregator<Long, Long> create() {
            return new Aggregator<Long, Long>() {

              private Long sum = null;

              @Override
              public void addImpl(Long x) {
                if (sum == null) {
                  sum = x;
                } else {
                  sum += x;
                }
              }

              @Override
              public Long finishImpl() {
                return sum;
              }

              @Override
              public boolean isNull() {
                return sum == null;
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'sum'.", argType), ident);
      }
    }
  }

  @Func(doc = "The average of the elements.")
  public static class Avg extends AggrFunction {
    public Avg() {
      super("avg");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new Aggregator<Double, Double>() {

              private Double sum = null;
              private Long cnt = 0L;

              @Override
              public void addImpl(Double x) {
                if (sum == null) {
                  sum = x;
                } else {
                  sum += x;
                }
                cnt++;
              }

              @Override
              public Double finishImpl() {
                return sum / cnt;
              }

              @Override
              public boolean isNull() {
                return sum == null;
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Long> create() {
            return new Aggregator<Double, Long>() {

              private Double sum = null;
              private Long cnt = 0L;

              @Override
              public void addImpl(Long x) {
                if (sum == null) {
                  sum = (double) x;
                } else {
                  sum += x;
                }
                cnt++;
              }

              @Override
              public Double finishImpl() {
                return sum / cnt;
              }

              @Override
              public boolean isNull() {
                return sum == null;
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'avg'.", argType), ident);
      }
    }
  }

  public static abstract class StdDevAggregator<T> extends Aggregator<Double, T> {

    protected Double sum1 = null;
    protected Double sum2 = null;
    protected Double sum0 = null;

    protected void addItem(Double x) {
      if (sum1 == null) {
        sum1 = 0.0;
        sum2 = 0.0;
        sum0 = 0.0;
      }
      sum1 += x;
      sum2 += x * x;
      sum0++;
    }

    @Override
    public boolean isNull() {
      return sum1 == null;
    }

    protected double sampleMoment2() {
      return (sum0 * sum2 - sum1 * sum1) / (sum0 * (sum0 - 1));
    }

    protected double sampleDev() {
      return Math.sqrt(sampleMoment2());
    }

    protected double popDev() {
      return Math.sqrt(sum0 * sum2 - sum1 * sum1) / sum0;
    }

    protected double popToSampleCoeff() {
      return (sum0 / (sum0 - 1));
    }
  }

  @Func(doc = "The sample standard deviation of the elements.")
  public static class StdDev extends AggrFunction {
    public static abstract class StdDevSampleAggregator<T> extends StdDevAggregator<T> {
      @Override
      public Double finishImpl() {
        return sampleDev();
      }
    }

    public StdDev() {
      super("stddev");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new StdDevSampleAggregator<Double>() {
              @Override
              public void addImpl(Double x) {
                addItem(x);
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Long> create() {
            return new StdDevSampleAggregator<Long>() {
              @Override
              public void addImpl(Long x) {
                addItem((double) x);
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'stddev'.", argType), ident);
      }
    }
  }

  @Func(doc = "The population standard deviation of the elements.")
  public static class StdDevPop extends AggrFunction {
    public static abstract class StdDevPopAggregator<T> extends StdDevAggregator<T> {
      @Override
      public Double finishImpl() {
        return popDev();
      }
    }

    public StdDevPop() {
      super("stddev_pop");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new StdDevPopAggregator<Double>() {
              @Override
              public void addImpl(Double x) {
                addItem(x);
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Long> create() {
            return new StdDevPopAggregator<Long>() {
              @Override
              public void addImpl(Long x) {
                addItem((double) x);
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'stddev_pop'.", argType),
            ident);
      }
    }
  }

  @Func(doc = "The sample skewness of the elements.")
  public static class Skewness extends AggrFunction {
    public static abstract class SkewnessAggregator<T> extends StdDevAggregator<T> {

      protected Double sum3 = null;

      protected void addItem(Double x) {
        super.addItem(x);
        if (sum3 == null) {
          sum3 = 0.0;
        }
        sum3 += x * x * x;
      }

      protected double sampleMoment3() {
        double mu = sum1 / sum0;
        double sigmaPop = popDev();
        return popToSampleCoeff() * (sum3 / sum0 - 3 * mu * sigmaPop * sigmaPop - mu * mu * mu);
      }

      @Override
      public Double finishImpl() {
        double sigmaSample = sampleDev();
        double sampleMoment3 = sampleMoment3();
        return sampleMoment3 / (sigmaSample * sigmaSample * sigmaSample);
      }
    }

    public Skewness() {
      super("skewness");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new SkewnessAggregator<Double>() {
              @Override
              public void addImpl(Double x) {
                addItem(x);
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Long> create() {
            return new SkewnessAggregator<Long>() {
              @Override
              public void addImpl(Long x) {
                addItem((double) x);
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'skewness'.", argType),
            ident);
      }
    }
  }

  @Func(doc = "The sample kurtosis of the elements.")
  public static class Kurtosis extends AggrFunction {
    public static abstract class KurtosisAggregator<T> extends SkewnessAggregator<T> {

      protected Double sum4 = null;

      protected void addItem(Double x) {
        super.addItem(x);
        if (sum4 == null) {
          sum4 = 0.0;
        }
        sum4 += x * x * x * x;
      }

      protected double sampleMoment4() {
        double mu = sum1 / sum0;
        return popToSampleCoeff() * (sum4 / sum0
            - 4 * (sum3 / sum0) * mu
            + 6 * (sum2 / sum0) * mu * mu
            - 4 * (sum1 / sum0) * mu * mu * mu
            + mu * mu * mu * mu);
      }

      @Override
      public Double finishImpl() {
        double sm2 = sampleMoment2();
        double sm4 = sampleMoment4();
        return sm4 / (sm2 * sm2);
      }
    }

    public Kurtosis() {
      super("kurtosis");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new KurtosisAggregator<Double>() {
              @Override
              public void addImpl(Double x) {
                addItem(x);
              }
            };
          }
        };
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Long> create() {
            return new KurtosisAggregator<Long>() {
              @Override
              public void addImpl(Long x) {
                addItem((double) x);
              }
            };
          }
        };
      } else {
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'kurtosis'.", argType),
            ident);
      }
    }
  }
}
