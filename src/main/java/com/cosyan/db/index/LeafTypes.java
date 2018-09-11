package com.cosyan.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.cosyan.db.index.ByteTrie.KeyType;
import com.cosyan.db.index.ByteTrie.ValueType;
import com.cosyan.db.io.Serializer;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class LeafTypes {

  public static KeyType<Long> longKeyType = new KeyType<Long>() {

    @Override
    public byte[] toByteArray(Long key) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(key);
      return buffer.array();
    }

    @Override
    public Long read(RandomAccessFile raf) throws IOException {
      return (Long) Serializer.readColumn(DataTypes.LongType, raf);
    }

    @Override
    public void write(DataOutputStream stream, Long key) throws IOException {
      Serializer.writeColumn(key, DataTypes.LongType, stream);
    }

    @Override
    public int size(Long key) {
      return Long.BYTES + 1;
    }

    @Override
    public boolean keysEqual(Long key1, Long key2) {
      return Objects.equals(key1, key2);
    }
  };

  public static KeyType<String> stringKeyType = new KeyType<String>() {

    @Override
    public byte[] toByteArray(String key) {
      ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES * key.length());
      buffer.asCharBuffer().put(key.toCharArray());
      return buffer.array();
    }

    @Override
    public String read(RandomAccessFile raf) throws IOException {
      return (String) Serializer.readColumn(DataTypes.StringType, raf);
    }

    @Override
    public void write(DataOutputStream stream, String key) throws IOException {
      Serializer.writeColumn(key, DataTypes.StringType, stream);
    }

    @Override
    public int size(String key) {
      return Character.BYTES * key.length() + 4 + 1;
    }

    @Override
    public boolean keysEqual(String key1, String key2) {
      return Objects.equals(key1, key2);
    }
  };

  public static KeyType<Double> doubleKeyType = new KeyType<Double>() {

    @Override
    public byte[] toByteArray(Double key) {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
      buffer.putDouble(key);
      return buffer.array();
    }

    @Override
    public Double read(RandomAccessFile raf) throws IOException {
      return (Double) Serializer.readColumn(DataTypes.DoubleType, raf);
    }

    @Override
    public void write(DataOutputStream stream, Double key) throws IOException {
      Serializer.writeColumn(key, DataTypes.DoubleType, stream);
    }

    @Override
    public int size(Double key) {
      return Double.BYTES + 1;
    }

    @Override
    public boolean keysEqual(Double key1, Double key2) {
      return Objects.equals(key1, key2);
    }
  };

  public static KeyType<Object[]> multiKeyType(final ImmutableList<DataType<?>> types) {
    return new KeyType<Object[]>() {

      @Override
      public Object[] read(RandomAccessFile raf) throws IOException {
        Object[] keys = new Object[types.size()];
        for (int i = 0; i < types.size(); i++) {
          keys[i] = Serializer.readColumn(types.get(i), raf);
        }
        return keys;
      }

      @Override
      public void write(DataOutputStream stream, Object[] key) throws IOException {
        serializeKey(key, stream);
      }

      private void serializeKey(Object[] key, DataOutputStream stream) throws IOException {
        for (int i = 0; i < types.size(); i++) {
          DataType<?> type = types.get(i);
          Serializer.writeColumn(key[i], type, stream);
        }
      }

      @Override
      public int size(Object[] key) {
        int size = types.size();
        for (int i = 0; i < types.size(); i++) {
          size += types.get(i).size(key[i]);
        }
        return size;
      }

      @Override
      public byte[] toByteArray(Object[] key) {
        ByteArrayOutputStream b = new ByteArrayOutputStream(256);
        DataOutputStream stream = new DataOutputStream(b);
        try {
          serializeKey(key, stream);
          return b.toByteArray();
        } catch (IOException e) {
          throw new RuntimeException(e); // Should not happen.
        }
      }

      @Override
      public boolean keysEqual(Object[] key1, Object[] key2) {
        for (int i = 0; i < types.size(); i++) {
          if (!Objects.equals(key1[i], key2[i])) {
            return false;
          }
        }
        return true;
      }
    };
  }

  private static final ValueType<Long> longValueType = new ValueType<Long>() {

    @Override
    public Long read(RandomAccessFile raf) throws IOException {
      return raf.readLong();
    }

    @Override
    public void write(DataOutputStream stream, Long value) throws IOException {
      stream.writeLong(value);
    }

    @Override
    public int size(Long value) {
      return Long.BYTES;
    }
  };

  public static class LongIndex extends ByteTrie<Long, Long> {
    public LongIndex(String fileName) throws IOException {
      super(fileName, longKeyType, longValueType);
    }
  }

  public static class StringIndex extends ByteTrie<String, Long> {
    public StringIndex(String fileName) throws IOException {
      super(fileName, stringKeyType, longValueType);
    }
  }

  public static class DoubleIndex extends ByteTrie<Double, Long> {
    public DoubleIndex(String fileName) throws IOException {
      super(fileName, doubleKeyType, longValueType);
    }
  }

  public static class MultiColumnIndex extends ByteTrie<Object[], Long> {
    public MultiColumnIndex(String fileName, ImmutableList<DataType<?>> types) throws IOException {
      super(fileName, multiKeyType(types), longValueType);
    }
  }
}
