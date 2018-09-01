/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.RuntimeIndexException;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Serializer;
import com.cosyan.db.model.DataTypes;

import lombok.Data;

public abstract class ByteMultiTrie<T> {

  private static final long[] EMPTY = new long[0];

  private static final int POINTERS_PER_NODE = 10;
  private static final int NODE_SIZE = Long.BYTES * (POINTERS_PER_NODE + 1);
  private static final long NULL_VALUE = Long.MIN_VALUE;

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
      Arrays.fill(values, NULL_VALUE);
    }

    public long getValue(int i) {
      return values[i];
    }
  }

  private ByteTrie<T, MultiLeaf> trie;

  private final String fileName;
  protected RandomAccessFile raf;
  private long filePointer;
  private long stableFilePointer;

  private final LinkedHashMap<Long, PendingNode> pendingNodes = new LinkedHashMap<>();

  protected ByteMultiTrie(String fileName, ByteTrie<T, MultiLeaf> trie) throws IOException {
    this.fileName = fileName;
    this.raf = new RandomAccessFile(fileName, "rw");
    this.trie = trie;
    if (!new File(fileName).exists() || raf.length() == 0) {
      // Let's not start indexing from 0, since that is the null file pointer.
      raf.write(0);
    }
    filePointer = raf.length();
    stableFilePointer = filePointer;
  }

  public void close() throws IOException {
    raf.close();
  }

  public void drop() throws IOException {
    trie.drop();
    close();
    new File(fileName).delete();
  }

  public void reOpen() throws FileNotFoundException {
    this.raf = new RandomAccessFile(fileName, "rw");
  }

  public void commit() throws IOException {
    trie.commit();
    for (Map.Entry<Long, PendingNode> node : pendingNodes.entrySet()) {
      saveNode(node.getKey(), node.getValue());
    }
    if (filePointer != raf.length()) {
      throw new RuntimeIndexException("Inconsistent state.");
    }
    pendingNodes.clear();
    stableFilePointer = filePointer;
  }

  private void saveNode(long filePointer, PendingNode node) throws IOException {
    raf.seek(filePointer);
    ByteBuffer bb = ByteBuffer.allocate(NODE_SIZE);
    LongBuffer lb = bb.asLongBuffer();
    lb.put(node.getNextPointer());
    lb.put(node.getValues());
    raf.write(bb.array());
  }

  public void rollback() {
    trie.rollback();
    filePointer = stableFilePointer;
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
        if (value != NULL_VALUE) {
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
      // Key doesn't exist.
      long newLeafPointer = filePointer;
      // Create new chain node.
      PendingNode node = new PendingNode();
      pendingNodes.put(newLeafPointer, node);
      filePointer += NODE_SIZE;
      // Save key in the index trie.
      leaf = new MultiLeaf(newLeafPointer, newLeafPointer);
      trie.put(key, leaf);
    }
    ChainNode node = loadNode(leaf.getLastIndex());
    int i;
    for (i = 0; i < POINTERS_PER_NODE; i++) {
      if (node.getValue(i) == NULL_VALUE) {
        break; // Found an empty slot in the last node.
      }
    }
    if (i < POINTERS_PER_NODE) {
      // Value can fit among the existing nodes.
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

      // Modify the next pointer of the previous chain node.
      if (node instanceof PendingNode) {
        ((PendingNode) node).setNextPointer(newNodePointer);
      } else {
        PendingNode newParentNode = new PendingNode();
        System.arraycopy(((ImmutableNode) node).getValues(), 0, newParentNode.getValues(), 0, POINTERS_PER_NODE);
        newParentNode.setNextPointer(newNodePointer);
        pendingNodes.put(leaf.getLastIndex(), newParentNode);
      }

      // Modify the last index of the key in the trie.
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
        // Delete the first matching value by setting it to null.
        if (value == valueToDelete) {
          if (node instanceof PendingNode) {
            ((PendingNode) node).getValues()[i] = NULL_VALUE;
          } else {
            PendingNode newNode = new PendingNode();
            System.arraycopy(((ImmutableNode) node).getValues(), 0, newNode.getValues(), 0, POINTERS_PER_NODE);
            newNode.getValues()[i] = NULL_VALUE;
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

  public ByteMultiTrieStat stats() throws IOException {
    ByteTrieStat trieStat = trie.stats();
    return new ByteMultiTrieStat(
        trieStat.getIndexFileSize(),
        raf.length(),
        trieStat.getInMemNodes(),
        trieStat.getPendingNodes(),
        pendingNodes.size());
  }

  private static class LongMultiLeafIndex extends ByteTrie<Long, MultiLeaf> {

    protected LongMultiLeafIndex(String fileName) throws IOException {
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
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
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

    @Override
    protected boolean keysEqual(Long key1, Long key2) {
      return Objects.equals(key1, key2);
    }
  }

  private static class StringMultiLeafIndex extends ByteTrie<String, MultiLeaf> {

    protected StringMultiLeafIndex(String fileName) throws IOException {
      super(fileName + "#index");
    }

    @Override
    protected byte[] toByteArray(String key) {
      ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES * key.length());
      buffer.asCharBuffer().put(key.toCharArray());
      return buffer.array();
    }

    @Override
    protected Leaf<String, MultiLeaf> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<String, MultiLeaf>(
          (String) Serializer.readColumn(DataTypes.StringType, raf),
          new MultiLeaf(raf.readLong(), raf.readLong()));
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<String, MultiLeaf> leaf) throws IOException {
      raf.seek(filePointer);
      ByteArrayOutputStream b = new ByteArrayOutputStream(leafSize(leaf));
      DataOutputStream stream = new DataOutputStream(b);
      Serializer.writeColumn(leaf.key(), DataTypes.StringType, stream);
      stream.writeLong(leaf.value().getFirstIndex());
      stream.writeLong(leaf.value().getLastIndex());
      raf.write(b.toByteArray());
    }

    @Override
    protected int leafSize(Leaf<String, MultiLeaf> leaf) {
      return Character.BYTES * leaf.key().length() + 4 + Long.BYTES * 2 + 1;
    }

    @Override
    protected boolean keysEqual(String key1, String key2) {
      return Objects.equals(key1, key2);
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
}
