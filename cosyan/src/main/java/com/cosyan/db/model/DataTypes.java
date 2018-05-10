package com.cosyan.db.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

public class DataTypes {

  @Data
  public static abstract class DataType<T> {
    private final String name;

    @Override
    public String toString() {
      return name;
    }

    public DataType<T[]> toListType() throws ModelException {
      return new DataType<T[]>(name + "_list") {

        @SuppressWarnings("unchecked")
        @Override
        public Class<T[]> javaClass() {
          return (Class<T[]>) Array.newInstance(DataType.this.javaClass(), 0).getClass();
        }

        @Override
        public void write(Object value, DataOutput stream) throws IOException {
          Object[] values = (Object[]) value;
          stream.writeInt(values.length);
          for (Object v : values) {
            DataType.this.write(v, stream);
          }
        }

        @SuppressWarnings("unchecked")
        @Override
        public T[] read(DataInput stream) throws IOException {
          int s = stream.readInt();
          Object[] array = new Object[s];
          for (int i = 0; i < s; i++) {
            array[i] = DataType.this.read(stream);
          }
          return (T[]) array;
        }

        @Override
        public int size(Object value) {
          Object[] values = (Object[]) value;
          int i = 4;
          for (Object v : values) {
            i += DataType.this.size(v);
          }
          return i;
        }
      };
    }

    public abstract Class<T> javaClass();

    public boolean isString() {
      return javaClass().equals(String.class);
    }

    public boolean isLong() {
      return javaClass().equals(Long.class);
    }

    public boolean isDouble() {
      return javaClass().equals(Double.class);
    }

    public boolean isDate() {
      return javaClass().equals(Date.class);
    }

    public boolean isBool() {
      return javaClass().equals(Boolean.class);
    }

    public boolean isNull() {
      return false;
    }

    public abstract void write(Object value, DataOutput stream) throws IOException;

    public abstract T read(DataInput stream) throws IOException;

    public abstract int size(Object value);

    public void check(Object value) throws RuleException {
    }

    public JSONObject toJSON() {
      return new JSONObject(ImmutableMap.of("type", name));
    }
  }

  public static final DataType<String> StringType = new DataType<String>("varchar") {

    @Override
    public Class<String> javaClass() {
      return String.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      String str = (String) value;
      stream.writeInt(str.length());
      stream.writeChars(str);
    }

    @Override
    public String read(DataInput stream) throws IOException {
      int length = stream.readInt();
      char[] chars = new char[length];
      for (int c = 0; c < chars.length; c++) {
        chars[c] = stream.readChar();
      }
      return new String(chars);
    }

    @Override
    public int size(Object value) {
      return 4 + ((String) value).length() * 2;
    }
  };

  public static final DataType<Double> DoubleType = new DataType<Double>("float") {

    @Override
    public Class<Double> javaClass() {
      return Double.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      stream.writeDouble((double) value);
    }

    @Override
    public Double read(DataInput stream) throws IOException {
      return stream.readDouble();
    }

    @Override
    public int size(Object value) {
      return 8;
    }
  };

  public static final DataType<Long> LongType = new DataType<Long>("integer") {

    @Override
    public Class<Long> javaClass() {
      return Long.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      stream.writeLong((long) value);
    }

    @Override
    public Long read(DataInput stream) throws IOException {
      return stream.readLong();
    }

    @Override
    public int size(Object value) {
      return 8;
    }
  };

  public static final DataType<Boolean> BoolType = new DataType<Boolean>("boolean") {

    @Override
    public Class<Boolean> javaClass() {
      return Boolean.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      stream.writeBoolean((boolean) value);
    }

    @Override
    public Boolean read(DataInput stream) throws IOException {
      return stream.readBoolean();
    }

    @Override
    public int size(Object value) {
      return 1;
    }
  };

  public static final DataType<Date> DateType = new DataType<Date>("timestamp") {

    @Override
    public Class<Date> javaClass() {
      return Date.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      stream.writeLong(((Date) value).getTime());
    }

    @Override
    public Date read(DataInput stream) throws IOException {
      return new Date(stream.readLong());
    }

    @Override
    public int size(Object value) {
      return 8;
    }
  };

  public static final DataType<Object> NullType = new DataType<Object>("null") {

    @Override
    public Class<Object> javaClass() {
      return Object.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date read(DataInput stream) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull() {
      return true;
    }
  };

  public static final DataType<Long> IDType = new DataType<Long>("id") {

    @Override
    public Class<Long> javaClass() {
      return Long.class;
    }

    @Override
    public void write(Object value, DataOutput stream) throws IOException {
      stream.writeLong((long) value);
    }

    @Override
    public Long read(DataInput stream) throws IOException {
      return stream.readLong();
    }

    @Override
    public int size(Object value) {
      return 8;
    }
  };

  public static DataType<?> enumType(JSONArray arr) {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < arr.length(); i++) {
      values.add(arr.getString(i));
    }
    return enumType(values);
  }

  public static DataType<?> enumType(final List<String> values) {

    ImmutableMap.Builder<String, Byte> builder = ImmutableMap.builder();
    for (byte b = 0; b < values.size(); b++) {
      builder.put(values.get(b), b);
    }
    final ImmutableMap<String, Byte> map = builder.build();
    return new DataType<String>("enum") {

      @Override
      public Class<String> javaClass() {
        return String.class;
      }

      @Override
      public void write(Object value, DataOutput stream) throws IOException {
        stream.writeByte(map.get((String) value));
      }

      @Override
      public String read(DataInput stream) throws IOException {
        return values.get(stream.readByte());
      }

      @Override
      public int size(Object value) {
        return 1;
      }

      @Override
      public JSONObject toJSON() {
        JSONObject obj = super.toJSON();
        obj.put("values", values);
        return obj;
      }

      @Override
      public void check(Object value) throws RuleException {
        if (!map.containsKey((String) value)) {
          throw new RuleException(String.format("Invalid enum value '%s'.", value));
        }
      }
    };
  }

  public static DataType<?> fromJSON(JSONObject obj) {
    String name = obj.getString("type");
    if (name.equals(StringType.getName())) {
      return StringType;
    } else if (name.equals(DoubleType.getName())) {
      return DoubleType;
    } else if (name.equals(LongType.getName())) {
      return LongType;
    } else if (name.equals(IDType.getName())) {
      return IDType;
    } else if (name.equals(DateType.getName())) {
      return DateType;
    } else if (name.equals("enum")) {
      return enumType(obj.getJSONArray("values"));
    }
    throw new IllegalArgumentException();
  }
}
