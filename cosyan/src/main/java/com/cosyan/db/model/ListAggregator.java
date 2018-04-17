package com.cosyan.db.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

@Func(doc = "Aggregates the elements into a list.")
public class ListAggregator extends AggrFunction {
  public ListAggregator() {
    super("list");
  }

  @Override
  public TypedAggrFunction<?> compile(DataType<?> argType) throws ModelException {
    if (argType == DataTypes.StringType) {
      return new TypedAggrFunction<String[]>(ident, DataTypes.StringListType) {

        @Override
        public Aggregator<String[], Object> create() {
          return new Aggregator<String[], Object>() {

            private ArrayList<String> list = new ArrayList<>();

            @Override
            public void addImpl(Object x) {
              list.add((String) x);
            }

            @Override
            public String[] finishImpl() {
              Object[] array = list.toArray();
              return Arrays.copyOf(array, array.length, String[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType == DataTypes.DoubleType) {
      return new TypedAggrFunction<Double[]>(ident, DataTypes.DoubleListType) {

        @Override
        public Aggregator<Double[], Object> create() {
          return new Aggregator<Double[], Object>() {

            private ArrayList<Double> list = new ArrayList<>();

            @Override
            public void addImpl(Object x) {
              list.add((Double) x);
            }

            @Override
            public Double[] finishImpl() {
              Object[] array = list.toArray();
              return Arrays.copyOf(array, array.length, Double[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType == DataTypes.LongType) {
      return new TypedAggrFunction<Long[]>(ident, DataTypes.LongListType) {

        @Override
        public Aggregator<Long[], Object> create() {
          return new Aggregator<Long[], Object>() {

            private ArrayList<Long> list = new ArrayList<>();

            @Override
            public void addImpl(Object x) {
              list.add((Long) x);
            }

            @Override
            public Long[] finishImpl() {
              Object[] array = list.toArray();
              return Arrays.copyOf(array, array.length, Long[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType == DataTypes.DateType) {
      return new TypedAggrFunction<Date[]>(ident, DataTypes.DateListType) {

        @Override
        public Aggregator<Date[], Object> create() {
          return new Aggregator<Date[], Object>() {

            private ArrayList<Date> list = new ArrayList<>();

            @Override
            public void addImpl(Object x) {
              list.add((Date) x);
            }

            @Override
            public Date[] finishImpl() {
              Object[] array = list.toArray();
              return Arrays.copyOf(array, array.length, Date[].class);
            }

            @Override
            public boolean isNull() {
              return false;
            }
          };
        }
      };
    } else if (argType == DataTypes.BoolType) {
      return new TypedAggrFunction<Boolean[]>(ident, DataTypes.BoolListType) {

        @Override
        public Aggregator<Boolean[], Object> create() {
          return new Aggregator<Boolean[], Object>() {

            private ArrayList<Boolean> list = new ArrayList<>();

            @Override
            public void addImpl(Object x) {
              list.add((Boolean) x);
            }

            @Override
            public Boolean[] finishImpl() {
              Object[] array = list.toArray();
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
      throw new ModelException(String.format("Unsupported data type %s for list aggregator.", argType));
    }
  }
}