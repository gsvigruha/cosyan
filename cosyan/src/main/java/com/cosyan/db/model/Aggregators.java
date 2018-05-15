package com.cosyan.db.model;

import java.util.Date;
import java.util.HashSet;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.doc.FunctionDocumentation.FuncCat;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

@FuncCat(name = "aggr", doc = "General aggregators")
public class Aggregators {

  public static abstract class Aggregator<T, U> {

    @SuppressWarnings("unchecked")
    public void add(Object x) {
      if (x != null) {
        addImpl((U) x);
      }
    }

    public abstract void addImpl(U x);

    public Object finish() {
      if (isNull()) {
        return null;
      } else {
        return finishImpl();
      }
    }

    public abstract T finishImpl();

    public abstract boolean isNull();
  }

  @Func(doc = "Counts the non `null` elements.")
  public static class Count extends AggrFunction {
    public Count() {
      super("count");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(name, DataTypes.LongType) {

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

  @Func(doc = "Counts the distinct non `null` elements.")
  public static class CountDistinct extends AggrFunction {
    public CountDistinct() {
      super("count$distinct");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(name, DataTypes.LongType) {

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

  @Func(doc = "The maximum of the elements.")
  public static class Max extends AggrFunction {
    public Max() {
      super("max");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

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
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Long>(name, DataTypes.LongType) {

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
      } else if (argType.isString()) {
        return new TypedAggrFunction<String>(name, DataTypes.StringType) {

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
      } else if (argType.isDate()) {
        return new TypedAggrFunction<Date>(name, DataTypes.DateType) {

          @Override
          public Aggregator<Date, Date> create() {
            return new Aggregator<Date, Date>() {

              private Date max = null;

              @Override
              public void addImpl(Date x) {
                if (max == null) {
                  max = x;
                } else {
                  max = max.compareTo(x) < 0 ? x : max;
                }
              }

              @Override
              public Date finishImpl() {
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
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'max'.", argType), ident);
      }
    }
  }

  @Func(doc = "The minimum of the elements.")
  public static class Min extends AggrFunction {
    public Min() {
      super("min");
    }

    @Override
    public TypedAggrFunction<?> compile(Ident ident, DataType<?> argType) throws ModelException {
      if (argType.isDouble()) {
        return new TypedAggrFunction<Double>(name, DataTypes.DoubleType) {

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
      } else if (argType.isLong()) {
        return new TypedAggrFunction<Long>(name, DataTypes.LongType) {

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
      } else if (argType.isString()) {
        return new TypedAggrFunction<String>(name, DataTypes.StringType) {

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
      } else if (argType.isDate()) {
        return new TypedAggrFunction<Date>(name, DataTypes.DateType) {

          @Override
          public Aggregator<Date, Date> create() {
            return new Aggregator<Date, Date>() {

              private Date min = null;

              @Override
              public void addImpl(Date x) {
                if (min == null) {
                  min = x;
                } else {
                  min = min.compareTo(x) > 0 ? x : min;
                }
              }

              @Override
              public Date finishImpl() {
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
        throw new ModelException(String.format("Invalid argument type '%s' for aggregator 'min'.", argType), ident);
      }
    }
  }
}
