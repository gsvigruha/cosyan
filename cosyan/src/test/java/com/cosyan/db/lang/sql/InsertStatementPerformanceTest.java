package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;

public class InsertStatementPerformanceTest extends UnitTestBase {

  private static final int N = 10000;

  @Test
  public void testInsertWithIndex() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index inserted in " + t);
  }

  @Test
  public void testInsertWithoutIndex() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index inserted in " + t);
  }
}
