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
package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Ignore;
import org.junit.Test;

public class MultipleRAFTest {

  @Ignore
  @Test
  public void testRAFs() throws IOException {
    RandomAccessFile f1 = new RandomAccessFile("/tmp/raf", "rw");
    for (int i = 0; i < 100; i++) {
      f1.write(i);
    }
    RandomAccessFile[] fs = new RandomAccessFile[100];
    for (int i = 0; i < 100; i++) {
      fs[i] = new RandomAccessFile("/tmp/raf", "rw");
    }
    for (int i = 0; i < 100; i++) {
      fs[i].seek(i);
    }
    for (int i = 0; i < 100; i++) {
      System.out.println(fs[i].read());
      fs[i].close();
    }
    f1.close();
  }
}
