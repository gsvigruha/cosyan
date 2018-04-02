package com.cosyan.db.model;

import java.util.HashSet;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

public class Aggregators {

  public static abstract class Aggregator<T, U> {

    @SuppressWarnings("unchecked")
    public void add(Object x) {
      if (x != DataTypes.NULL) {
        addImpl((U) x);
      }
    }

    public abstract void addImpl(U x);

    public Object finish() {
      if (isNull()) {
        return DataTypes.NULL;
      } else {
        return finishImpl();
      }
    }

    public abstract T finishImpl();

    public abstract boolean isNull();
  }

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
    protected Long cnt = 0L;

    protected void addItem(Double x) {
      if (sum == null) {
        sum = 0.0;
        sum2 = 0.0;
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

  public static class Count extends AggrFunction {
    public Count() {
      super("count");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {

        @Override
        public Aggregator<Long, Object> create() {
          return new Aggregator<Long, Object>() {

            private long sum = 0L;

            @Override
            public void addImpl(Object x) {
              sum++;
            }

            @Override
            public Long finishImpl() {
              return sum;
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    }
  }

  public static class CountDistinct extends AggrFunction {
    public CountDistinct() {
      super("count$distinct");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {

        @Override
        public Aggregator<Long, Object> create() {
          return new Aggregator<Long, Object>() {

            private HashSet<Object> set = new HashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add(x);
            }

            @Override
            public Long finishImpl() {
              return (long) set.size();
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    }
  }

  public static class Max extends AggrFunction {
    public Max() {
      super("max");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new Aggregator<Double, Double>() {

              private Double max = null;

              @Override
              public void addImpl(Double x) {
                if (max == null) {
                  max = x;
                } else {
                  max = Math.max(max, x);
                }
              }

              @Override
              public Double finishImpl() {
                return max;
              }

              @Override
              public boolean isNull() {
                return max == null;
              }
            };
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {

          @Override
          public Aggregator<Long, Long> create() {
            return new Aggregator<Long, Long>() {

              private Long max = null;

              @Override
              public void addImpl(Long x) {
                if (max == null) {
                  max = x;
                } else {
                  max = Math.max(max, x);
                }
              }

              @Override
              public Long finishImpl() {
                return max;
              }

              @Override
              public boolean isNull() {
                return max == null;
              }
            };
          }
        };
      } else if (argType == DataTypes.StringType) {
        return new TypedAggrFunction<String>(ident, DataTypes.StringType) {

          @Override
          public Aggregator<String, String> create() {
            return new Aggregator<String, String>() {

              private String max = null;

              @Override
              public void addImpl(String x) {
                if (max == null) {
                  max = x;
                } else {
                  max = max.compareTo(x) < 0 ? x : max;
                }
              }

              @Override
              public String finishImpl() {
                return max;
              }

              @Override
              public boolean isNull() {
                return max == null;
              }
            };
          }
        };
      } else {
        throw new ModelException("Invalid type for max: '" + argType + "'.");
      }
    }
  }

  public static class Min extends AggrFunction {
    public Min() {
      super("min");
    }

    @Override
    public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {

          @Override
          public Aggregator<Double, Double> create() {
            return new Aggregator<Double, Double>() {

              private Double min = null;

              @Override
              public void addImpl(Double x) {
                if (min == null) {
                  min = x;
                } else {
                  min = Math.min(min, x);
                }
              }

              @Override
              public Double finishImpl() {
                return min;
              }

              @Override
              public boolean isNull() {
                return min == null;
              }
            };
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {

          @Override
          public Aggregator<Long, Long> create() {
            return new Aggregator<Long, Long>() {

              private Long min = null;

              @Override
              public void addImpl(Long x) {
                if (min == null) {
                  min = x;
                } else {
                  min = Math.min(min, x);
                }
              }

              @Override
              public Long finishImpl() {
                return min;
              }

              @Override
              public boolean isNull() {
                return min == null;
              }
            };
          }
        };
      } else if (argType == DataTypes.StringType) {
        return new TypedAggrFunction<String>(ident, DataTypes.StringType) {

          @Override
          public Aggregator<String, String> create() {
            return new Aggregator<String, String>() {

              private String min = null;

              @Override
              public void addImpl(String x) {
                if (min == null) {
                  min = x;
                } else {
                  min = min.compareTo(x) > 0 ? x : min;
                }
              }

              @Override
              public String finishImpl() {
                return min;
              }

              @Override
              public boolean isNull() {
                return min == null;
              }
            };
          }
        };
      } else {
        throw new ModelException("Invalid type for min: '" + argType + "'.");
      }
    }
  }
}
