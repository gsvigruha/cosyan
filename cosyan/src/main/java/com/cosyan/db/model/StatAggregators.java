package com.cosyan.db.model;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

public class StatAggregators {
  public static class Sum extends AggrFunction {
    public Sum() {
      super("sum");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {

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
        throw new ModelException("Invalid type for sum: '" + argType + "'.");
      }
    }
  }

  public static class Avg extends AggrFunction {
    public Avg() {
      super("avg");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
        throw new ModelException("Invalid type for avg: '" + argType + "'.");
      }
    }
  }

  public static abstract class StdDevAggregator<T> extends Aggregator<Double, T> {

    protected Double sum = null;
    protected Double sum2 = null;
    protected Double cnt = null;

    protected void addItem(Double x) {
      if (sum == null) {
        sum = 0.0;
        sum2 = 0.0;
        cnt = 0.0;
      }
      sum += x;
      sum2 += x * x;
      cnt++;
    }

    @Override
    public boolean isNull() {
      return sum == null;
    }
  }

  public static class StdDev extends AggrFunction {
    public static abstract class StdDevSampleAggregator<T> extends StdDevAggregator<T> {
      @Override
      public Double finishImpl() {
        return Math.sqrt((cnt * sum2 - sum * sum) / (cnt * (cnt - 1)));
      }
    }

    public StdDev() {
      super("stddev");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
        throw new ModelException("Invalid type for stddev: '" + argType + "'.");
      }
    }
  }

  public static class StdDevPop extends AggrFunction {
    public static abstract class StdDevPopAggregator<T> extends StdDevAggregator<T> {
      @Override
      public Double finishImpl() {
        return Math.sqrt(cnt * sum2 - sum * sum) / cnt;
      }
    }

    public StdDevPop() {
      super("stddev_pop");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
        throw new ModelException("Invalid type for stddev_pop: '" + argType + "'.");
      }
    }
  }

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

      @Override
      public Double finishImpl() {
        double mu = sum / cnt;
        double sigmaPop = Math.sqrt(cnt * sum2 - sum * sum) / cnt;
        double sigmaSample = Math.sqrt((cnt * sum2 - sum * sum) / (cnt * (cnt - 1)));
        double samplePopRatio = (cnt / (cnt - 1));
        return samplePopRatio * ((sum3 / cnt - 3 * mu * sigmaPop * sigmaPop - mu * mu * mu)
            / (sigmaSample * sigmaSample * sigmaSample));
      }
    }

    public Skewness() {
      super("skewness");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

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
        throw new ModelException("Invalid type for skewness: '" + argType + "'.");
      }
    }
  }
}
