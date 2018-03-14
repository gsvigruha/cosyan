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
