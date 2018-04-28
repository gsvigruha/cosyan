package com.cosyan.db.model;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

@Func(doc = "Aggregates the elements into a list containing only unique elements.")
public class SetAggregator extends AggrFunction {
  public SetAggregator() {
    super("set");
  }

  @Override
  public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
    if (argType.isString()) {
      return new TypedAggrFunction<String[]>(ident, DataTypes.StringType.toListType()) {

        @Override
        public Aggregator<String[], Object> create() {
          return new Aggregator<String[], Object>() {

            private LinkedHashSet<String> set = new LinkedHashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add((String) x);
            }

            @Override
            public String[] finishImpl() {
              Object[] array = set.toArray();
              return Arrays.copyOf(array, array.length, String[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType.isDouble()) {
      return new TypedAggrFunction<Double[]>(ident, DataTypes.DoubleType.toListType()) {

        @Override
        public Aggregator<Double[], Object> create() {
          return new Aggregator<Double[], Object>() {

            private LinkedHashSet<Double> set = new LinkedHashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add((Double) x);
            }

            @Override
            public Double[] finishImpl() {
              Object[] array = set.toArray();
              return Arrays.copyOf(array, array.length, Double[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType.isLong()) {
      return new TypedAggrFunction<Long[]>(ident, DataTypes.LongType.toListType()) {

        @Override
        public Aggregator<Long[], Object> create() {
          return new Aggregator<Long[], Object>() {

            private LinkedHashSet<Long> set = new LinkedHashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add((Long) x);
            }

            @Override
            public Long[] finishImpl() {
              Object[] array = set.toArray();
              return Arrays.copyOf(array, array.length, Long[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType.isDate()) {
      return new TypedAggrFunction<Date[]>(ident, DataTypes.DateType.toListType()) {

        @Override
        public Aggregator<Date[], Object> create() {
          return new Aggregator<Date[], Object>() {

            private LinkedHashSet<Date> set = new LinkedHashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add((Date) x);
            }

            @Override
            public Date[] finishImpl() {
              Object[] array = set.toArray();
              return Arrays.copyOf(array, array.length, Date[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType.isBool()) {
      return new TypedAggrFunction<Boolean[]>(ident, DataTypes.BoolType.toListType()) {

        @Override
        public Aggregator<Boolean[], Object> create() {
          return new Aggregator<Boolean[], Object>() {

            private LinkedHashSet<Boolean> set = new LinkedHashSet<>();

            @Override
            public void addImpl(Object x) {
              set.add((Boolean) x);
            }

            @Override
            public Boolean[] finishImpl() {
              Object[] array = set.toArray();
              return Arrays.copyOf(array, array.length, Boolean[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported data type %s for set aggregator.", argType));
    }
  }
}