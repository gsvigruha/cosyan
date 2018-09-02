package com.cosyan.db.index;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.cosyan.db.index.ByteMultiTrie.MultiLeaf;
import com.cosyan.db.index.ByteTrie.LeafType;
import com.cosyan.db.index.LongLeafTries.DoubleKeyIndex;
import com.cosyan.db.index.LongLeafTries.LongKeyIndex;
import com.cosyan.db.index.LongLeafTries.MultiColumnKeyIndex;
import com.cosyan.db.index.LongLeafTries.StringKeyIndex;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class MultiLeafTries {
  private static final LeafType<MultiLeaf> multiLeafType = new LeafType<MultiLeaf>() {

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

  private static class LongMultiLeafIndex extends LongKeyIndex<MultiLeaf> {
    protected LongMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", multiLeafType);
    }
  }

  private static class StringMultiLeafIndex extends StringKeyIndex<MultiLeaf> {
    protected StringMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", multiLeafType);
    }
  }

  private static class DoubleMultiLeafIndex extends DoubleKeyIndex<MultiLeaf> {
    protected DoubleMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index", multiLeafType);
    }
  }

  private static class MultiColumnMultiLeafIndex extends MultiColumnKeyIndex<MultiLeaf> {
    protected MultiColumnMultiLeafIndex(String fileName, ImmutableList<DataType<?>> types) throws IOException {
      super(fileName + "#index", types, multiLeafType);
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
