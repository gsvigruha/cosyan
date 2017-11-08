package com.cosyan.db.model;

import java.util.Date;

import com.cosyan.db.meta.MetaRepo.ModelException;

import lombok.Data;

public class DataTypes {

  @Data
  public static abstract class DataType<T> {
    private final String name;

    @Override
    public String toString() {
      return name;
    }

    public abstract DataType<?> toListType() throws ModelException;
  }

  public static final DataType<String> StringType = new DataType<String>("varchar") {
    @Override
    public DataType<?> toListType() {
      return StringListType;
    }
  };

  public static final DataType<Double> DoubleType = new DataType<Double>("float") {
    @Override
    public DataType<?> toListType() {
      return DoubleListType;
    }
  };

  public static final DataType<Long> LongType = new DataType<Long>("integer") {
    @Override
    public DataType<?> toListType() {
      return LongListType;
    }
  };

  public static final DataType<Boolean> BoolType = new DataType<Boolean>("boolean") {
    @Override
    public DataType<?> toListType() {
      return BoolListType;
    }
  };

  public static final DataType<Date> DateType = new DataType<Date>("timestamp") {
    @Override
    public DataType<?> toListType() {
      return DateListType;
    }
  };

  public static final DataType<String[]> StringListType = new DataType<String[]>("varchar_list") {
    @Override
    public DataType<?> toListType() throws ModelException {
      throw new ModelException("Cannot create a list of lists.");
    }
  };

  public static final DataType<Double[]> DoubleListType = new DataType<Double[]>("float_list") {
    @Override
    public DataType<?> toListType() throws ModelException {
      throw new ModelException("Cannot create a list of lists.");
    }
  };

  public static final DataType<Long[]> LongListType = new DataType<Long[]>("integer_list") {
    @Override
    public DataType<?> toListType() throws ModelException {
      throw new ModelException("Cannot create a list of lists.");
    }
  };

  public static final DataType<Boolean[]> BoolListType = new DataType<Boolean[]>("boolean_list") {
    @Override
    public DataType<?> toListType() throws ModelException {
      throw new ModelException("Cannot create a list of lists.");
    }
  };

  public static final DataType<Date[]> DateListType = new DataType<Date[]>("timestamp_list") {
    @Override
    public DataType<?> toListType() throws ModelException {
      throw new ModelException("Cannot create a list of lists.");
    }
  };

  public static final class NullType implements Comparable<Object> {
    @Override
    public boolean equals(Object o) {
      return o == NULL;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public int compareTo(Object o) {
      return o == NULL ? 0 : -1;
    }

    @Override
    public String toString() {
      return "null";
    }
  }

  public static final NullType NULL = new NullType();

  public static DataType<?> fromString(String name) {
    if (name.equals("varchar")) {
      return StringType;
    } else if (name.equals("float")) {
      return DoubleType;
    } else if (name.equals("integer")) {
      return LongType;
    } else if (name.equals("boolean")) {
      return BoolType;
    } else if (name.equals("timestamp")) {
      return DateType;
    } else if (name.equals("varchar_list")) {
      return StringListType;
    } else if (name.equals("float_list")) {
      return DoubleListType;
    } else if (name.equals("integer_list")) {
      return LongListType;
    } else if (name.equals("boolean_list")) {
      return BoolListType;
    } else if (name.equals("timestamp_list")) {
      return DateListType;
    } else {
      throw new IllegalArgumentException();
    }
  }
}
