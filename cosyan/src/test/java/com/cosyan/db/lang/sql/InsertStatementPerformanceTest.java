package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;

public class InsertStatementPerformanceTest extends UnitTestBase {

  private static final int N = 20000;
  private static final int T = 1000;

  @Test
  public void testInsertWithIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index one per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithoutIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index one per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithIndex_multipleRecordsPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t3 (a varchar unique, b integer);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      int x = i * T;
      sb.append("insert into t3 values ('abc" + x + "' ," + x + ")");
      for (int j = 1; j < T; j++) {
        x++;
        sb.append(",('abc" + x + "' ," + x + ")");
      }
      sb.append(";");
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index many per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithoutIndex_multipleRecordsPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t4 (a varchar, b integer);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      int x = i * T;
      sb.append("insert into t4 values ('abc" + x + "' ," + x + ")");
      for (int j = 1; j < T; j++) {
        x++;
        sb.append(",('abc" + x + "' ," + x + ")");
      }
      sb.append(";");
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index many per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithIDIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t5 (a id, b varchar);");
    for (int i = 0; i < N; i++) {
      execute("insert into t5 values ('abc" + i + "');");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ID index one per transaction inserted in " + t + " " + speed(t, N));
  }
}
