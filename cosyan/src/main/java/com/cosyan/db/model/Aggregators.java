package com.cosyan.db.model;

import java.util.HashSet;
import java.util.Set;

import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo.ModelException;

public class Aggregators {

  public static class Sum extends AggrFunction {
    public Sum() {
      super("sum");
    }

    @Override
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {
          @Override
          public Double aggregateImpl(Object a, Object x) {
            return (Double) a + (Double) x;
          }

          @Override
          public Double init() {
            return 0.0;
          }

          @Override
          public Double finish(Object x) {
            return (Double) x;
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {
          @Override
          public Long aggregateImpl(Object a, Object x) {
            return (Long) a + (Long) x;
          }

          @Override
          public Long init() {
            return 0L;
          }

          @Override
          public Long finish(Object x) {
            return (Long) x;
          }
        };
      } else {
        throw new ModelException("Invalid type for sum: '" + argType + "'.");
      }
    }
  }

  public static class Count extends AggrFunction {
    public Count() {
      super("count");
    }

    @Override
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {
        @Override
        public Long aggregateImpl(Object a, Object x) {
          return (Long) a + 1L;
        }

        @Override
        public Long init() {
          return 0L;
        }

        @Override
        public Long finish(Object x) {
          return (Long) x;
        }
      };
    }
  }

  public static class CountDistinct extends AggrFunction {
    public CountDistinct() {
      super("count$distinct");
    }

    @Override
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {
        @Override
        public Object aggregateImpl(Object a, Object x) {
          ((HashSet<Object>) a).add(x);
          return a;
        }

        @Override
        public Set<Object> init() {
          return new HashSet<>();
        }

        @Override
        public Long finish(Object x) {
          return (long) ((HashSet<?>) x).size();
        }
      };
    }
  }

  public static class Max extends AggrFunction {
    public Max() {
      super("max");
    }

    @Override
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {
          @Override
          public Double aggregateImpl(Object a, Object x) {
            return Math.max((Double) a, (Double) x);
          }

          @Override
          public Double init() {
            return Double.MIN_VALUE;
          }

          @Override
          public Double finish(Object x) {
            return (Double) x;
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {
          @Override
          public Long aggregateImpl(Object a, Object x) {
            return Math.max((Long) a, (Long) x);
          }

          @Override
          public Long init() {
            return Long.MIN_VALUE;
          }

          @Override
          public Long finish(Object x) {
            return (Long) x;
          }
        };
      } else if (argType == DataTypes.StringType) {
        return new TypedAggrFunction<String>(ident, DataTypes.StringType) {
          @Override
          public String aggregateImpl(Object a, Object x) {
            return a == null ? (String) x : ((String) a).compareTo((String) x) > 0 ? (String) a : (String) x;
          }

          @Override
          public String init() {
            return null;
          }

          @Override
          public String finish(Object x) {
            return (String) x;
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
    public TypedAggrFunction<?> forType(DataType<?> argType) throws ModelException {
      if (argType == DataTypes.DoubleType) {
        return new TypedAggrFunction<Double>(ident, DataTypes.DoubleType) {
          @Override
          public Double aggregateImpl(Object a, Object x) {
            return Math.min((Double) a, (Double) x);
          }

          @Override
          public Double init() {
            return Double.MAX_VALUE;
          }

          @Override
          public Double finish(Object x) {
            return (Double) x;
          }
        };
      } else if (argType == DataTypes.LongType) {
        return new TypedAggrFunction<Long>(ident, DataTypes.LongType) {
          @Override
          public Long aggregateImpl(Object a, Object x) {
            return Math.min((Long) a, (Long) x);
          }

          @Override
          public Long init() {
            return Long.MAX_VALUE;
          }

          @Override
          public Long finish(Object x) {
            return (Long) x;
          }
        };
      } else if (argType == DataTypes.StringType) {
        return new TypedAggrFunction<String>(ident, DataTypes.StringType) {
          @Override
          public String aggregateImpl(Object a, Object x) {
            return a == null ? (String) x : ((String) a).compareTo((String) x) < 0 ? (String) a : (String) x;
          }

          @Override
          public String init() {
            return null;
          }

          @Override
          public String finish(Object x) {
            return (String) x;
          }
        };
      } else {
        throw new ModelException("Invalid type for min: '" + argType + "'.");
      }
    }
  }
}
