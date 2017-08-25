package com.cosyan.db.index;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;

public class ByteTrieTest {

  @Test
  public void testLongByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longindex"));
    LongIndex index = new LongIndex("/tmp/longindex");
    assertEquals(-1, index.get(1L));
    index.put(1L, 10L);
    assertEquals(10L, index.get(1L));
    index.put(2L, 20L);
    assertEquals(20L, index.get(2L));
    index.put(3L, 30L);
    assertEquals(30L, index.get(3L));
    index.put(999999999L, 40L);
    assertEquals(40L, index.get(999999999L));
    assertEquals(20L, index.get(2L));
    assertEquals(30L, index.get(3L));
    assertEquals(10L, index.get(1L));
    assertEquals(-1, index.get(666L));

    index.cleanUp();
    assertEquals(10L, index.get(1L));
    assertEquals(20L, index.get(2L));
    assertEquals(30L, index.get(3L));
    assertEquals(40L, index.get(999999999L));
    assertEquals(-1, index.get(666L));

    index.put(100000L, 50L);
    assertEquals(50L, index.get(100000L));
    
    assertEquals(true, index.delete(3L));
    assertEquals(-1, index.get(3L));
    assertEquals(false, index.delete(5L));
    assertEquals(true, index.delete(2L));
    assertEquals(-1, index.get(2L));
    
    assertEquals(10L, index.get(1L));
    assertEquals(40L, index.get(999999999L));
    assertEquals(50L, index.get(100000L));
  }

  @Test
  public void testStringByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringindex"));
    StringIndex index = new StringIndex("/tmp/stringindex");
    assertEquals(-1, index.get("a"));
    index.put("a", 10L);
    assertEquals(10L, index.get("a"));
    index.put("aa", 20L);
    assertEquals(20L, index.get("aa"));
    index.put("aaa", 30L);
    assertEquals(30L, index.get("aaa"));
    index.put("x", 40L);
    assertEquals(40L, index.get("x"));
    assertEquals(20L, index.get("aa"));
    assertEquals(30L, index.get("aaa"));
    assertEquals(10L, index.get("a"));
    assertEquals(-1, index.get("zzz"));

    index.cleanUp();
    assertEquals(10L, index.get("a"));
    assertEquals(20L, index.get("aa"));
    assertEquals(30L, index.get("aaa"));
    assertEquals(40L, index.get("x"));
    assertEquals(-1, index.get("zzz"));

    index.put("xxxxxx", 50L);
    assertEquals(50L, index.get("xxxxxx"));
    
    assertEquals(true, index.delete("aa"));
    assertEquals(-1, index.get("aa"));
    assertEquals(false, index.delete("b"));
    assertEquals(true, index.delete("a"));
    assertEquals(-1, index.get("a"));
    
    assertEquals(30L, index.get("aaa"));
    assertEquals(40L, index.get("x"));
    assertEquals(-1, index.get("zzz"));
    assertEquals(50L, index.get("xxxxxx"));    
  }

  @Test(expected = IndexException.class)
  public void testStringByteTrieDuplicateKeys() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringindex"));
    StringIndex index = new StringIndex("/tmp/stringindex");
    assertEquals(-1, index.get("a"));
    index.put("a", 10L);
    assertEquals(10L, index.get("a"));
    index.put("a", 10L);
  }

  @Test(expected = IndexException.class)
  public void testLongByteTrieDuplicateKeys() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longindex"));
    LongIndex index = new LongIndex("/tmp/longindex");
    assertEquals(-1, index.get(1L));
    index.put(1L, 10L);
    assertEquals(10L, index.get(1L));
    index.put(1L, 10L);
  }
}
