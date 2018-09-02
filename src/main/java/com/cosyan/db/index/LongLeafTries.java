package com.cosyan.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.cosyan.db.index.ByteTrie.LeafType;
import com.cosyan.db.io.Serializer;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class LongLeafTries {

  public static class LongKeyIndex<V> extends ByteTrie<Long, V> {

    public LongKeyIndex(String fileName, LeafType<V> leafType) throws IOException {
      super(fileName, leafType);
    }

    @Override
    protected byte[] toByteArray(Long key) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(key);
      return buffer.array();
    }

    @Override
    protected Leaf<Long, V> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<Long, V>((Long) Serializer.readColumn(DataTypes.LongType, raf), leafType.read(raf));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<Long, V> leaf) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
      DataOutputStream stream = new DataOutputStream(b);
      Serializer.writeColumn(leaf.key(), DataTypes.LongType, stream);
      raf.seek(filePointer);
      leafType.write(stream, leaf.value());
      raf.write(b.toByteArray());
    }

    @Override
    protected int leafSize(Leaf<Long, V> leaf) {
      return Long.BYTES + 1 + leafType.size(leaf.value());
    }

    @Override
    protected boolean keysEqual(Long key1, Long key2) {
      return Objects.equals(key1, key2);
    }
  }

  public static class StringKeyIndex<V> extends ByteTrie<String, V> {

    public StringKeyIndex(String fileName, LeafType<V> leafType) throws IOException {
      super(fileName, leafType);
    }

    @Override
    protected byte[] toByteArray(String key) {
      ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES * key.length());
      buffer.asCharBuffer().put(key.toCharArray());
      return buffer.array();
    }

    @Override
    protected Leaf<String, V> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<String, V>((String) Serializer.readColumn(DataTypes.StringType, raf), leafType.read(raf));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<String, V> leaf) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
      DataOutputStream stream = new DataOutputStream(b);
      Serializer.writeColumn(leaf.key(), DataTypes.StringType, stream);
      raf.seek(filePointer);
      leafType.write(stream, leaf.value());
      raf.write(b.toByteArray());
    }

    @Override
    protected int leafSize(Leaf<String, V> leaf) {
      return Character.BYTES * leaf.key().length() + 4 + 1 + leafType.size(leaf.value());
    }

    @Override
    protected boolean keysEqual(String key1, String key2) {
      return Objects.equals(key1, key2);
    }
  }

  public static class DoubleKeyIndex<V> extends ByteTrie<Double, V> {

    public DoubleKeyIndex(String fileName, LeafType<V> leafType) throws IOException {
      super(fileName, leafType);
    }

    @Override
    protected byte[] toByteArray(Double key) {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
      buffer.putDouble(key);
      return buffer.array();
    }

    @Override
    protected Leaf<Double, V> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<Double, V>((Double) Serializer.readColumn(DataTypes.DoubleType, raf), leafType.read(raf));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<Double, V> leaf) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
      DataOutputStream stream = new DataOutputStream(b);
      Serializer.writeColumn(leaf.key(), DataTypes.DoubleType, stream);
      raf.seek(filePointer);
      leafType.write(stream, leaf.value());
      raf.write(b.toByteArray());
    }

    @Override
    protected int leafSize(Leaf<Double, V> leaf) {
      return Double.BYTES + 1 + leafType.size(leaf.value());
    }

    @Override
    protected boolean keysEqual(Double key1, Double key2) {
      return Objects.equals(key1, key2);
    }
  }

  public static class MultiColumnKeyIndex<V> extends ByteTrie<Object[], V> {

    private final ImmutableList<DataType<?>> types;

    public MultiColumnKeyIndex(String fileName, ImmutableList<DataType<?>> types, LeafType<V> leafType) throws IOException {
      super(fileName, leafType);
      this.types = types;
    }

    @Override
    protected Leaf<Object[], V> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      Object[] keys = new Object[types.size()];
      for (int i = 0; i < types.size(); i++) {
        keys[i] = Serializer.readColumn(types.get(i), raf);
      }
      return new Leaf<Object[], V>(keys, leafType.read(raf));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<Object[], V> leaf) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
      DataOutputStream stream = new DataOutputStream(b);
      serializeKey(leaf.key(), stream);
      raf.seek(filePointer);
      leafType.write(stream, leaf.value());
      raf.write(b.toByteArray());
    }

    private void serializeKey(Object[] key, DataOutputStream stream) throws IOException {
      for (int i = 0; i < types.size(); i++) {
        DataType<?> type = types.get(i);
        Serializer.writeColumn(key[i], type, stream);
      }
    }

    @Override
    protected int leafSize(Leaf<Object[], V> leaf) {
      int size = leafType.size(leaf.value()) + types.size();
      for (int i = 0; i < types.size(); i++) {
        size += types.get(i).size(leaf.key()[i]);
      }
      return size;
    }

    @Override
    protected byte[] toByteArray(Object[] key) {
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
    protected boolean keysEqual(Object[] key1, Object[] key2) {
      for (int i = 0; i < types.size(); i++) {
        if (!Objects.equals(key1[i], key2[i])) {
          return false;
        }
      }
      return true;
    }
  }

  private static final LeafType<Long> longLeafType = new LeafType<Long>() {

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

  public static class LongIndex extends LongKeyIndex<Long> {
    public LongIndex(String fileName) throws IOException {
      super(fileName, longLeafType);
    }
  }

  public static class StringIndex extends StringKeyIndex<Long> {
    public StringIndex(String fileName) throws IOException {
      super(fileName, longLeafType);
    }
  }

  public static class DoubleIndex extends DoubleKeyIndex<Long> {
    public DoubleIndex(String fileName) throws IOException {
      super(fileName, longLeafType);
    }
  }

  public static class MultiColumnIndex extends MultiColumnKeyIndex<Long> {
    public MultiColumnIndex(String fileName, ImmutableList<DataType<?>> types) throws IOException {
      super(fileName, types, longLeafType);
    }
  }
}
