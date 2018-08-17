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

import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.cosyan.db.index.ByteTrie.IndexException;

public class IDIndexTest {

  private void assertEquals(Long expected, Long actual) {
    org.junit.Assert.assertEquals(expected, actual);
  }

  private void assertEquals(boolean expected, boolean actual) {
    org.junit.Assert.assertEquals(expected, actual);
  }

  @Test
  public void testIDIndex() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/idindex"));
    IDIndex index = new IDIndex("/tmp/idindex");
    assertEquals(null, index.get(1L));
    index.put(1L, 10L);
    index.commit();
    assertEquals(10L, index.get(1L));
    index.put(2L, 20L);
    index.commit();
    assertEquals(20L, index.get(2L));
    index.put(3L, 30L);
    index.commit();
    assertEquals(30L, index.get(3L));
    index.put(9999999L, 40L);
    index.commit();
    assertEquals(40L, index.get(9999999L));
    assertEquals(20L, index.get(2L));
    assertEquals(30L, index.get(3L));
    assertEquals(10L, index.get(1L));
    assertEquals(null, index.get(666L));

    index.put(100000L, 50L);
    assertEquals(50L, index.get(100000L));

    assertEquals(true, index.delete(3L));
    index.commit();
    assertEquals(null, index.get(3L));
    assertEquals(false, index.delete(5L));
    index.commit();
    assertEquals(true, index.delete(2L));
    index.commit();
    assertEquals(null, index.get(2L));

    assertEquals(10L, index.get(1L));
    assertEquals(40L, index.get(9999999L));
    assertEquals(50L, index.get(100000L));
  }

  @Test
  public void testIDIndexCommitAndRollback() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/idindex"));
    IDIndex index = new IDIndex("/tmp/idindex");
    assertEquals(null, index.get(1L));
    index.put(10L, 10L);
    index.put(30L, 30L);
    index.put(20L, 20L);
    assertEquals(10L, index.get(10L));
    assertEquals(20L, index.get(20L));
    assertEquals(30L, index.get(30L));
    index.commit();
    assertEquals(10L, index.get(10L));
    assertEquals(20L, index.get(20L));
    assertEquals(30L, index.get(30L));

    index.put(50L, 50L);
    index.put(40L, 40L);
    assertEquals(10L, index.get(10L));
    assertEquals(20L, index.get(20L));
    assertEquals(30L, index.get(30L));
    assertEquals(40L, index.get(40L));
    assertEquals(50L, index.get(50L));
    index.rollback();

    assertEquals(10L, index.get(10L));
    assertEquals(20L, index.get(20L));
    assertEquals(30L, index.get(30L));
    assertEquals(null, index.get(40L));
    assertEquals(null, index.get(50L));

    index.delete(20L);
    assertEquals(10L, index.get(10L));
    assertEquals(null, index.get(20L));
    assertEquals(30L, index.get(30L));
    index.commit();
    assertEquals(10L, index.get(10L));
    assertEquals(null, index.get(20L));
    assertEquals(30L, index.get(30L));

    index.delete(10L);
    assertEquals(null, index.get(10L));
    assertEquals(30L, index.get(30L));

    index.rollback();
    assertEquals(10L, index.get(10L));
    assertEquals(30L, index.get(30L));

    index.put(60L, 60L);
    assertEquals(60L, index.get(60L));
    index.delete(60L);
    assertEquals(null, index.get(60L));
    index.commit();
    assertEquals(null, index.get(60L));
  }

  @Test
  public void testIDIndexDuplicateKeys() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/idindex"));
    IDIndex index = new IDIndex("/tmp/idindex");
    assertEquals(null, index.get(1L));
    index.put(1L, 10L);
    assertEquals(10L, index.get(1L));
    try {
      index.put(1L, 10L);
      fail();
    } catch (IndexException e) {
    }
  }

  @Test
  public void testIDIndexSegmentBoundaries() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/idindex"));
    IDIndex index = new IDIndex("/tmp/idindex");
    assertEquals(null, index.get(0L));
    index.put(0L, 10L);
    assertEquals(10L, index.get(0L));
    assertEquals(null, index.get(4096L));
  }
}
