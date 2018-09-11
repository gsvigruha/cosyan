package com.cosyan.db.index;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.cosyan.db.index.ByteMultiTrie.MultiLeaf;
import com.cosyan.db.index.ByteTrie.ValueType;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class MultiLeafTries {
  private static final ValueType<MultiLeaf> multiLeafValueType = new ValueType<MultiLeaf>() {

    @Override
    public MultiLeaf read(RandomAccessFile raf) throws IOException {
      return new MultiLeaf(raf.readLong(), raf.readLong());
    }

    @Override
    public void write(DataOutputStream stream, MultiLeaf value) throws IOException {
      stream.writeLong(value.getFirstIndex());
      stream.writeLong(value.getLastIndex());
    }

    @Override
    public int size(MultiLeaf value) {
      return Long.BYTES * 2;
    }
  };

  private static class LongMultiLeafIndex extends ByteTrie<Long, MultiLeaf> {
    protected LongMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", LeafTypes.longKeyType, multiLeafValueType);
    }
  }

  private static class StringMultiLeafIndex extends ByteTrie<String, MultiLeaf> {
    protected StringMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", LeafTypes.stringKeyType, multiLeafValueType);
    }
  }

  private static class DoubleMultiLeafIndex extends ByteTrie<Double, MultiLeaf> {
    protected DoubleMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", LeafTypes.doubleKeyType, multiLeafValueType);
    }
  }

  private static class MultiColumnMultiLeafIndex extends ByteTrie<Object[], MultiLeaf> {
    protected MultiColumnMultiLeafIndex(String fileName, ImmutableList<DataType<?>> types) throws IOException {
      super(fileName + "#index", LeafTypes.multiKeyType(types), multiLeafValueType);
    }
  }

  public static class LongMultiIndex extends ByteMultiTrie<Long> {
    public LongMultiIndex(String fileName) throws IOException {
      super(fileName + "#chain", new LongMultiLeafIndex(fileName));
    }
  }

  public static class StringMultiIndex extends ByteMultiTrie<String> {
    public StringMultiIndex(String fileName) throws IOException {
      super(fileName + "#chain", new StringMultiLeafIndex(fileName));
    }
  }

  public static class DoubleMultiIndex extends ByteMultiTrie<Double> {
    public DoubleMultiIndex(String fileName) throws IOException {
      super(fileName + "#chain", new DoubleMultiLeafIndex(fileName));
    }
  }

  public static class MultiColumnMultiIndex extends ByteMultiTrie<Object[]> {
    public MultiColumnMultiIndex(String fileName, ImmutableList<DataType<?>> types) throws IOException {
      super(fileName + "#chain", new MultiColumnMultiLeafIndex(fileName, types));
    }
  }
}
