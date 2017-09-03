package com.cosyan.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.RuntimeIndexException;
import com.cosyan.db.io.Serializer;
import com.cosyan.db.model.DataTypes;

import lombok.Data;

public abstract class ByteMultiTrie<T> {

  private static final long[] EMPTY = new long[0];

  private static final int POINTERS_PER_NODE = 10;
  private static final int NODE_SIZE = Long.BYTES * (POINTERS_PER_NODE + 1);

  @Data
  private static class MultiLeaf {
    private final long firstIndex;
    private final long lastIndex;
  }

  private static interface ChainNode {

    public long getValue(int i);

    public long getNextPointer();
  }

  @Data
  private static class ImmutableNode implements ChainNode {
    private final long nextPointer;
    private final long[] values;

    public long getValue(int i) {
      return values[i];
    }
  }

  @Data
  private static class PendingNode implements ChainNode {
    private long nextPointer;
    private long[] values;

    public PendingNode() {
      nextPointer = 0L;
      values = new long[POINTERS_PER_NODE];
    }

    public long getValue(int i) {
      return values[i];
    }
  }

  private ByteTrie<T, MultiLeaf> trie;

  private final String fileName;
  protected RandomAccessFile raf;
  private long filePointer;

  private final LinkedHashMap<Long, PendingNode> pendingNodes = new LinkedHashMap<>();

  protected ByteMultiTrie(String fileName, ByteTrie<T, MultiLeaf> trie) throws IOException {
    this.fileName = fileName;
    this.raf = new RandomAccessFile(fileName, "rw");
    this.trie = trie;
    // Let's not start indexing from 0, since that is the null file pointer.
    raf.write(0);
    this.filePointer = raf.length();
  }

  public void commit() throws IOException {
    trie.commit();
    for (Map.Entry<Long, PendingNode> node : pendingNodes.entrySet()) {
      raf.seek(node.getKey());
      ByteBuffer bb = ByteBuffer.allocate(NODE_SIZE);
      LongBuffer lb = bb.asLongBuffer();
      lb.put(node.getValue().getNextPointer());
      lb.put(node.getValue().getValues());
      raf.write(bb.array());
    }
    pendingNodes.clear();
    if (filePointer != raf.length()) {
      throw new RuntimeIndexException("Inconsistent state.");
    }
  }

  public void rollback() throws IOException {
    trie.rollback();
    filePointer = raf.length();
    pendingNodes.clear();
  }

  private ChainNode loadNode(long id) throws IOException {
    PendingNode pendingNode = pendingNodes.get(id);
    if (pendingNode != null) {
      return pendingNode;
    }
    raf.seek(id);
    ByteBuffer bb = ByteBuffer.allocate(NODE_SIZE);
    raf.read(bb.array());
    LongBuffer lb = bb.asLongBuffer();
    long[] values = new long[POINTERS_PER_NODE];
    long nextPointer = lb.get();
    lb.get(values);
    return new ImmutableNode(nextPointer, values);
  }

  public long[] get(T key) throws IOException {
    MultiLeaf leaf = trie.get(key);
    if (leaf == null) {
      return EMPTY;
    }
    List<Long> result = new LinkedList<>();
    long nextPointer = leaf.getFirstIndex();
    while (nextPointer > 0) {
      ChainNode node = loadNode(nextPointer);
      for (int i = 0; i < POINTERS_PER_NODE; i++) {
        long value = node.getValue(i);
        if (value > 0) {
          result.add(value);
        }
      }
      nextPointer = node.getNextPointer();
    }
    return result.stream().mapToLong(Long::longValue).toArray();
  }

  public void put(T key, long finalIndex) throws IOException, IndexException {
    MultiLeaf leaf = trie.get(key);
    if (leaf == null) {
      long newLeafPointer = filePointer;
      leaf = new MultiLeaf(newLeafPointer, newLeafPointer);
      PendingNode node = new PendingNode();
      pendingNodes.put(newLeafPointer, node);
      filePointer += NODE_SIZE;
      trie.put(key, leaf);
    }
    ChainNode node = loadNode(leaf.getLastIndex());
    int i;
    for (i = 0; i < POINTERS_PER_NODE; i++) {
      if (node.getValue(i) == 0) {
        break;
      }
    }
    if (i < POINTERS_PER_NODE) {
      if (node instanceof PendingNode) {
        ((PendingNode) node).getValues()[i] = finalIndex;
      } else {
        PendingNode newNode = new PendingNode();
        System.arraycopy(((ImmutableNode) node).getValues(), 0, newNode.getValues(), 0, POINTERS_PER_NODE);
        newNode.getValues()[i] = finalIndex;
        pendingNodes.put(leaf.getLastIndex(), newNode);
      }
    } else {
      // Need to add new node to the chain.
      PendingNode newNode = new PendingNode();
      newNode.getValues()[0] = finalIndex;
      long newNodePointer = filePointer;
      pendingNodes.put(newNodePointer, newNode);
      filePointer += NODE_SIZE;

      if (node instanceof PendingNode) {
        ((PendingNode) node).setNextPointer(newNodePointer);
      } else {
        PendingNode newParentNode = new PendingNode();
        System.arraycopy(((ImmutableNode) node), 0, newParentNode.getValues(), 0, POINTERS_PER_NODE);
        newParentNode.setNextPointer(newNodePointer);
        pendingNodes.put(leaf.getLastIndex(), newNode);
      }

      trie.delete(key);
      trie.put(key, new MultiLeaf(leaf.getFirstIndex(), newNodePointer));
    }
  }

  public boolean delete(T key) throws IOException {
    return trie.delete(key);
  }

  public boolean delete(T key, long valueToDelete) throws IOException {
    MultiLeaf leaf = trie.get(key);
    if (leaf == null) {
      return false;
    }
    long nextPointer = leaf.getFirstIndex();
    while (nextPointer > 0) {
      ChainNode node = loadNode(nextPointer);
      for (int i = 0; i < POINTERS_PER_NODE; i++) {
        long value = node.getValue(i);
        if (value == valueToDelete) {
          if (node instanceof PendingNode) {
            ((PendingNode) node).getValues()[i] = 0;
          } else {
            PendingNode newNode = new PendingNode();
            System.arraycopy(((ImmutableNode) node).getValues(), 0, newNode.getValues(), 0, POINTERS_PER_NODE);
            newNode.getValues()[i] = 0;
            newNode.setNextPointer(node.getNextPointer());
            pendingNodes.put(nextPointer, newNode);
          }
          return true;
        }
      }
      nextPointer = node.getNextPointer();
    }
    return false;
  }

  private static class MultiLeafIndex extends ByteTrie<Long, MultiLeaf> {

    protected MultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index");
    }

    @Override
    protected byte[] toByteArray(Long key) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(key);
      return buffer.array();
    }

    @Override
    protected Leaf<Long, MultiLeaf> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<Long, MultiLeaf>(
          (Long) Serializer.readColumn(DataTypes.LongType, raf),
          new MultiLeaf(raf.readLong(), raf.readLong()));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<Long, MultiLeaf> leaf) throws IOException {
      raf.seek(filePointer);
      ByteArrayOutputStream b = new ByteArrayOutputStream(Long.BYTES * 3 + 1);
      DataOutputStream stream = new DataOutputStream(b);
      Serializer.writeColumn(leaf.key(), DataTypes.LongType, stream);
      stream.writeLong(leaf.value().getFirstIndex());
      stream.writeLong(leaf.value().getLastIndex());
      raf.write(b.toByteArray());
    }

    @Override
    protected int leafSize(Leaf<Long, MultiLeaf> leaf) {
      return Long.BYTES * 3 + 1;
    }
  }

  public static class LongMultiIndex extends ByteMultiTrie<Long> {
    protected LongMultiIndex(String fileName) throws IOException {
      super(fileName + "#chain", new MultiLeafIndex(fileName));
    }
  }
}
