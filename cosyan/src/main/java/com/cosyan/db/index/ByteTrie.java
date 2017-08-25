package com.cosyan.db.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

import com.cosyan.db.io.Serializer;
import com.cosyan.db.model.DataTypes;

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
 * @param <T>
 *          type of the index.
 */
public abstract class ByteTrie<T> {

  /**
   * The keys array is addressed by a byte. The last slot is for elements matching
   * the current prefix.
   */
  private static final int KEYS_SIZE = 257;

  private static class Node<T> {
    protected int accesses = 0;
  }

  private static class Index<T> extends Node<T> {
    private final long[] keys;

    private Index(long[] keys) {
      this.keys = keys;
    }

    private long[] keys() {
      accesses++;
      return keys;
    }
  }

  protected static class Leaf<T> extends Node<T> {
    private final T key;
    private final long index;

    protected Leaf(T key, long index) {
      this.key = key;
      this.index = index;
    }

    protected T key() {
      accesses++;
      return key;
    }

    protected long index() {
      return index;
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

  private final String fileName;
  protected RandomAccessFile raf;

  protected final long[] root = new long[KEYS_SIZE];
  private final HashMap<Long, Node<T>> trie = new HashMap<>();

  protected ByteTrie(String fileName) throws IOException {
    this.fileName = fileName;
    this.raf = new RandomAccessFile(fileName, "rw");
    saveIndex(0, root);
  }

  public void close() throws IOException {
    raf.close();
  }

  public void reOpen() throws FileNotFoundException {
    this.raf = new RandomAccessFile(fileName, "rw");
  }

  protected abstract Leaf<T> loadLeaf(long filePointer) throws IOException;

  protected abstract void saveLeaf(long filePointer, Leaf<T> leaf) throws IOException;

  protected abstract byte[] toByteArray(T key);

  private Leaf<T> getLeaf(long id) throws IOException {
    Leaf<T> leafNode = (Leaf<T>) trie.get(id);
    if (leafNode == null) {
      // Assume leaf node exists but not in memory.
      if (id >= raf.length()) {
        throw new RuntimeIndexException("Inconsistent state.");
      }
      leafNode = loadLeaf(id);
      trie.put(id, leafNode);
    }
    return leafNode;
  }

  private Index<T> getIndex(long id) throws IOException {
    Index<T> indexNode = (Index<T>) trie.get(id);
    long fileIndex = -id;
    if (indexNode == null) {
      // Index node exists but not in memory.
      if (fileIndex >= raf.length()) {
        throw new RuntimeIndexException("Inconsistent state.");
      }
      raf.seek(fileIndex);
      long[] keysToLoad = new long[KEYS_SIZE];
      for (int j = 0; j < KEYS_SIZE; j++) {
        keysToLoad[j] = raf.readLong();
      }
      indexNode = new Index<T>(keysToLoad);
      trie.put(id, indexNode);
    }
    return indexNode;
  }

  public void cleanUp() {
    trie.clear();
  }

  protected long get(long[] pointers, byte[] keyBytes, int keyBytesIndex, T keyObject)
      throws IOException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        Leaf<T> leaf = getLeaf(currentKey);
        if (keyObject.equals(leaf.key())) {
          return leaf.index();
        } else {
          throw new RuntimeIndexException("Inconsistent state.");
        }
      } else {
        return -1;
      }
    }
    int keyByte = keyBytes[keyBytesIndex] - Byte.MIN_VALUE;
    long pointer = pointers[keyByte];
    if (pointer == 0) {
      // Equivalent of null pointer, search is over.
      return -1;
    } else if (pointer < 0) {
      // Pointer to index node.
      Index<T> nextIndex = getIndex(pointer);
      return get(nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject);
    } else {
      // Pointer to leaf node.
      Leaf<T> leaf = getLeaf(pointer);
      if (keyObject.equals(leaf.key())) {
        return leaf.index();
      } else {
        // Search is over and not found.
        return -1;
      }
    }
  }

  protected void put(long parentPointer, long[] pointers, byte[] keyBytes, int keyBytesIndex, T keyObject,
      long objectIndex)
      throws IOException, IndexException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        throw new IndexException("Key '" + keyObject + "' already present in index.");
      } else {
        Leaf<T> leaf = new Leaf<T>(keyObject, objectIndex);
        long fileIndex = raf.length();
        saveLeaf(fileIndex, leaf);
        trie.put(fileIndex, leaf);

        // Modify and save parent index.
        pointers[256] = fileIndex;
        saveIndex(parentPointer, pointers);
        return;
      }
    }
    int keyByte = keyBytes[keyBytesIndex] - Byte.MIN_VALUE;
    long pointer = pointers[keyByte];
    if (pointer == 0) {
      // Null pointer, create a leaf node.
      Leaf<T> leaf = new Leaf<T>(keyObject, objectIndex);
      long fileIndex = raf.length();
      saveLeaf(fileIndex, leaf);
      trie.put(fileIndex, leaf);

      // Modify and save parent index.
      pointers[keyByte] = fileIndex;
      saveIndex(parentPointer, pointers);
      return;
    } else if (pointer < 0) {
      // Pointer to index node.
      Index<T> nextIndex = getIndex(pointer);
      put(pointer, nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject, objectIndex);
    } else {
      // Pointer to leaf node.
      Leaf<T> leaf = getLeaf(pointer);
      if (keyObject.equals(leaf.key())) {
        // Already present, throw exception.
        throw new IndexException("Key '" + keyObject + "' already present in index.");
      } else {
        // Another non-final leaf node is present, need to split.
        // Create a new index node.
        long indexPointer = -raf.length();
        long[] newIndex = new long[KEYS_SIZE];
        byte[] existingKeyBytes = toByteArray(leaf.key());
        if (keyBytesIndex + 1 < existingKeyBytes.length) {
          // Push the existing key down the trie if has more bytes.
          newIndex[existingKeyBytes[keyBytesIndex + 1] - Byte.MIN_VALUE] = pointer;
        } else {
          // Add to the new index if has no more bytes.
          newIndex[256] = pointer;
        }
        saveIndex(indexPointer, newIndex);
        trie.put(indexPointer, new Index<T>(newIndex));

        // Modify and save parent index.
        pointers[keyByte] = indexPointer;
        saveIndex(parentPointer, pointers);

        put(indexPointer, newIndex, keyBytes, keyBytesIndex + 1, keyObject, objectIndex);
      }
    }
  }

  protected boolean delete(long parentPointer, long[] pointers, byte[] keyBytes, int keyBytesIndex, T keyObject)
      throws IOException {
    if (keyBytesIndex >= keyBytes.length) {
      // Check current node.
      long currentKey = pointers[256];
      if (currentKey > 0) {
        Leaf<T> leaf = getLeaf(currentKey);
        if (keyObject.equals(leaf.key())) {
          pointers[256] = 0;
          saveIndex(parentPointer, pointers);
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
      Index<T> nextIndex = getIndex(pointer);
      return delete(pointer, nextIndex.keys(), keyBytes, keyBytesIndex + 1, keyObject);
    } else {
      // Pointer to leaf node.
      Leaf<T> leaf = getLeaf(pointer);
      if (keyObject.equals(leaf.key())) {
        pointers[keyByte] = 0;
        saveIndex(parentPointer, pointers);
        return true;
      } else {
        // Search is over and not found.
        return false;
      }
    }
  }

  private void saveIndex(long id, long[] indices) throws IOException {
    raf.seek(-id);
    for (long l : indices) {
      raf.writeLong(l);
    }
  }

  public static class LongIndex extends ByteTrie<Long> {

    public LongIndex(String fileName) throws IOException {
      super(fileName);
    }

    @Override
    protected byte[] toByteArray(Long key) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(key);
      return buffer.array();
    }

    public long get(long key) throws IOException {
      return get(root, toByteArray(key), 0, key);
    }

    public void put(long key, long finalIndex) throws IOException, IndexException {
      put(0, root, toByteArray(key), 0, key, finalIndex);
    }

    public boolean delete(long key) throws IOException {
      return delete(0, root, toByteArray(key), 0, key);
    }

    @Override
    protected Leaf<Long> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<Long>((Long) Serializer.readColumn(DataTypes.LongType, raf), raf.readLong());
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<Long> leaf) throws IOException {
      raf.seek(filePointer);
      Serializer.writeColumn(leaf.key(), DataTypes.LongType, raf);
      raf.writeLong(leaf.index());
    }
  }

  public static class StringIndex extends ByteTrie<String> {

    public StringIndex(String fileName) throws IOException {
      super(fileName);
    }

    @Override
    protected byte[] toByteArray(String key) {
      ByteBuffer buffer = ByteBuffer.allocate(Character.BYTES * key.length());
      buffer.asCharBuffer().put(key.toCharArray());
      return buffer.array();
    }

    public long get(String key) throws IOException {
      return get(root, toByteArray(key), 0, key);
    }

    public void put(String key, long finalIndex) throws IOException, IndexException {
      put(0, root, toByteArray(key), 0, key, finalIndex);
    }

    public boolean delete(String key) throws IOException {
      return delete(0, root, toByteArray(key), 0, key);
    }

    @Override
    protected Leaf<String> loadLeaf(long filePointer) throws IOException {
      raf.seek(filePointer);
      return new Leaf<String>((String) Serializer.readColumn(DataTypes.StringType, raf), raf.readLong());
    }

    @Override
    protected void saveLeaf(long filePointer, Leaf<String> leaf) throws IOException {
      raf.seek(filePointer);
      Serializer.writeColumn(leaf.key(), DataTypes.StringType, raf);
      raf.writeLong(leaf.index());
    }
  }
}
