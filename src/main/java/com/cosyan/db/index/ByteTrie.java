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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.cosyan.db.index.IndexStat.ByteTrieStat;

/**
 * A prefix trie for indexing. Supports in memory caching to minimize file
 * accesses.
 * 
 * Every key is serialized to a byte array. The first <code>n</code> bytes are
 * used to address various levels of the prefix tree. Leaf nodes store the final
 * keys. In addition, every middle "index" node can store a key too, if the
 * bytes of the key exactly add up to the prefix.
 * 
 * Subclass this class for to implement for various key types.
 * 
 * @author gsvigruha
 *
 * @param <K, V> type of the index.
 */
public abstract class ByteTrie<K, V> {

  /**
   * The keys array is addressed by a byte. The last slot is for elements matching
   * the current prefix.
   */
  private static final int KEYS_SIZE = 257;

  public static class Node<K, V> {
    protected int accesses = 0;

    public int getAccesses() {
      return accesses;
    }
  }

  private static class Index<K, V> extends Node<K, V> {
    private final long[] keys;

    private Index(long[] keys) {
      this.keys = keys;
    }

    private long[] keys() {
      accesses++;
      return keys;
    }
  }

  protected static class Leaf<K, V> extends Node<K, V> {
    private final K key;
    private final V value;

    protected Leaf(K key, V value) {
      this.key = key;
      this.value = value;
    }

    protected K key() {
      accesses++;
      return key;
    }

    protected V value() {
      return value;
    }
  }

  public static class IndexException extends Exception {
    private static final long serialVersionUID = 1L;

    public IndexException(String msg) {
      super(msg);
    }
  }

  public static class RuntimeIndexException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RuntimeIndexException(String msg) {
      super(msg);
    }
  }

  public static abstract class LeafType<T> {

    public abstract T read(RandomAccessFile raf) throws IOException;

    public abstract void write(DataOutputStream stream, T value) throws IOException;

    public abstract int size(T value);

  }

  private final String fileName;
  protected final LeafType<V> leafType;
  protected RandomAccessFile raf;
  private long filePointer;
  private long stableFilePointer;

  private final HashMap<Long, Node<K, V>> trie = new HashMap<>();
  private final LinkedHashMap<Long, Node<K, V>> pendingNodes = new LinkedHashMap<>();

  protected ByteTrie(String fileName, LeafType<V> leafType) throws IOException {
    this.fileName = fileName;
    this.leafType = leafType;
    this.raf = new RandomAccessFile(fileName, "rw");
    if (!new File(fileName).exists() || raf.length() == 0) {
      saveIndex(0, new long[KEYS_SIZE]);
    }
    filePointer = raf.length();
    stableFilePointer = filePointer;
  }

  public synchronized void close() throws IOException {
    cleanUp();
    raf.close();
  }

  public synchronized void cleanUp() {
    trie.clear();
  }

  public synchronized void drop() throws IOException {
    close();
    new File(fileName).delete();
  }

  public synchronized void reOpen() throws FileNotFoundException {
    this.raf = new RandomAccessFile(fileName, "rw");
  }

  public synchronized V get(K key) throws IOException {
    return get(getIndex(0L).keys(), toByteArray(key), 0, key);
  }

  public synchronized void put(K key, V value) throws IOException, IndexException {
    put(0, getIndex(0L).keys(), toByteArray(key), 0, key, value);
  }

  public synchronized boolean delete(K key) throws IOException {
    return delete(0, getIndex(0L).keys(), toByteArray(key), 0, key);
  }

  public synchronized void commit() throws IOException {
    for (Map.Entry<Long, Node<K, V>> node : pendingNodes.entrySet()) {
      if (node.getKey() <= 0) {
        saveIndex(node.getKey(), ((Index<K, V>) node.getValue()).keys());
      } else {
        saveLeaf(node.getKey(), ((Leaf<K, V>) node.getValue()));
      }
    }
    long rafLength = raf.length();
    if (filePointer != rafLength) {
      throw new RuntimeIndexException(String.format("Inconsistent state: '%s' != '%s'.", filePointer, rafLength));
    }
    trie.putAll(pendingNodes);
    pendingNodes.clear();
    stableFilePointer = filePointer;
  }

  public synchronized void rollback() {
    filePointer = stableFilePointer;
    pendingNodes.clear();
  }

  protected abstract Leaf<K, V> loadLeaf(long filePointer) throws IOException;

  protected abstract void saveLeaf(long filePointer, Leaf<K, V> leaf) throws IOException;

  protected abstract int leafSize(Leaf<K, V> leaf);

  protected abstract byte[] toByteArray(K key);

  protected abstract boolean keysEqual(K key1, K key2);

  private Leaf<K, V> getLeaf(long id) throws IOException {
    Leaf<K, V> leafNode = (Leaf<K, V>) pendingNodes.get(id);
    if (leafNode != null) {
      return leafNode;
    }
    leafNode = (Leaf<K, V>) trie.get(id);
    if (leafNode == null) {
      // Assume leaf node exists but not in memory.
      if (id >= stableFilePointer) {
        throw new RuntimeIndexException("Inconsistent state.");
      }
      leafNode = loadLeaf(id);
      trie.put(id, leafNode);
    }
    return leafNode;
  }

  private Index<K, V> getIndex(long id) throws IOException {
    Index<K, V> indexNode = (Index<K, V>) pendingNodes.get(id);
    if (indexNode != null) {
      return indexNode;
    }
    indexNode = (Index<K, V>) trie.get(id);
    if (indexNode == null) {
      long fileIndex = -id;
      // Index node exists but not in memory.
      if (fileIndex >= stableFilePointer) {
        throw new RuntimeIndexException("Inconsistent state.");
      }
      raf.seek(fileIndex);
      ByteBuffer bb = ByteBuffer.allocate(KEYS_SIZE * Long.BYTES);
      raf.read(bb.array());
      long[] keysToLoad = new long[KEYS_SIZE];
      bb.asLongBuffer().get(keysToLoad);

      indexNode = new Index<K, V>(keysToLoad);
      trie.put(id, indexNode);
    }
    return indexNode;
  }

  private void modifyIndex(long parentPointer, long[] pointers, int i, long fileIndex) {
    long[] newPointers = pointers.clone();
    newPointers[i] = fileIndex;
    Index<K, V> newIndex = new Index<>(newPointers);
    pendingNodes.put(parentPointer, newIndex);
  }

  private void saveIndex(long id, long[] indices) throws IOException {
    ByteBuffer bb = ByteBuffer.allocate(KEYS_SIZE * 8);
    bb.asLongBuffer().put(indices);
    raf.seek(-id);
    raf.write(bb.array());
  }

  public synchronized void cleanUp(int limit) {
    Iterator<Map.Entry<Long, Node<K, V>>> iter = trie.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Long, Node<K, V>> item = iter.next();
      if (item.getValue().getAccesses() < limit) {
        iter.remove();
      }
    }
  }

  protected V get(long[] pointers, byte[] keyBytes, int keyBytesIndex, K keyObject)
      throws IOException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        Leaf<K, V> leaf = getLeaf(currentKey);
        if (keysEqual(keyObject, leaf.key())) {
          return leaf.value();
        } else {
          throw new RuntimeIndexException("Inconsistent state.");
        }
      } else {
        return null;
      }
    }
    int keyByte = keyBytes[keyBytesIndex] - Byte.MIN_VALUE;
    long pointer = pointers[keyByte];
    if (pointer == 0) {
      // Equivalent of null pointer, search is over.
      return null;
    } else if (pointer < 0) {
      // Pointer to index node.
      Index<K, V> nextIndex = getIndex(pointer);
      return get(nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject);
    } else {
      // Pointer to leaf node.
      Leaf<K, V> leaf = getLeaf(pointer);
      if (keysEqual(keyObject, leaf.key())) {
        return leaf.value();
      } else {
        // Search is over and not found.
        return null;
      }
    }
  }

  protected void put(long parentPointer, long[] pointers, byte[] keyBytes, int keyBytesIndex, K keyObject,
      V valueObject)
      throws IOException, IndexException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        throw new IndexException("Key '" + keyObject + "' already present in index.");
      } else {
        Leaf<K, V> leaf = new Leaf<K, V>(keyObject, valueObject);
        long fileIndex = filePointer;
        pendingNodes.put(fileIndex, leaf);
        filePointer += leafSize(leaf);

        // Modify parent index.
        modifyIndex(parentPointer, pointers, 256, fileIndex);
        return;
      }
    }
    int keyByte = keyBytes[keyBytesIndex] - Byte.MIN_VALUE;
    long pointer = pointers[keyByte];
    if (pointer == 0) {
      // Null pointer, create a leaf node.
      Leaf<K, V> leaf = new Leaf<K, V>(keyObject, valueObject);
      long fileIndex = filePointer;
      pendingNodes.put(fileIndex, leaf);
      filePointer += leafSize(leaf);

      // Modify parent index.
      modifyIndex(parentPointer, pointers, keyByte, fileIndex);
      return;
    } else if (pointer < 0) {
      // Pointer to index node.
      Index<K, V> nextIndex = getIndex(pointer);
      put(pointer, nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject, valueObject);
    } else {
      // Pointer to leaf node.
      Leaf<K, V> leaf = getLeaf(pointer);
      if (keysEqual(keyObject, leaf.key())) {
        // Already present, throw exception.
        throw new IndexException("Key '" + keyObject + "' already present in index.");
      } else {
        // Another non-final leaf node is present, need to split.
        // Create a new index node.
        long indexPointer = -filePointer;
        long[] newIndex = new long[KEYS_SIZE];
        byte[] existingKeyBytes = toByteArray(leaf.key());
        if (keyBytesIndex + 1 < existingKeyBytes.length) {
          // Push the existing key down the trie if has more bytes.
          newIndex[existingKeyBytes[keyBytesIndex + 1] - Byte.MIN_VALUE] = pointer;
        } else {
          // Add to the new index if has no more bytes.
          newIndex[256] = pointer;
        }
        // Add new index.
        pendingNodes.put(indexPointer, new Index<K, V>(newIndex));
        filePointer += KEYS_SIZE * Long.BYTES;

        // Modify parent index.
        modifyIndex(parentPointer, pointers, keyByte, indexPointer);

        put(indexPointer, newIndex, keyBytes, keyBytesIndex + 1, keyObject, valueObject);
      }
    }
  }

  protected boolean delete(
      long parentPointer,
      long[] pointers,
      byte[] keyBytes,
      int keyBytesIndex,
      K keyObject)
      throws IOException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        Leaf<K, V> leaf = getLeaf(currentKey);
        if (keysEqual(keyObject, leaf.key())) {
          modifyIndex(parentPointer, pointers, 256, 0);
          return true;
        } else {
          throw new RuntimeIndexException("Inconsistent state.");
        }
      } else {
        return false;
      }
    }
    int keyByte = keyBytes[keyBytesIndex] - Byte.MIN_VALUE;
    long pointer = pointers[keyByte];
    if (pointer == 0) {
      // Equivalent of null pointer, search is over.
      return false;
    } else if (pointer < 0) {
      // Pointer to index node.
      Index<K, V> nextIndex = getIndex(pointer);
      return delete(pointer, nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject);
    } else {
      // Pointer to leaf node.
      Leaf<K, V> leaf = getLeaf(pointer);
      if (keysEqual(keyObject, leaf.key())) {
        modifyIndex(parentPointer, pointers, keyByte, 0);
        return true;
      } else {
        // Search is over and not found.
        return false;
      }
    }
  }

  public ByteTrieStat stats() throws IOException {
    return new ByteTrieStat(raf.length(), trie.size(), pendingNodes.size());
  }
}
