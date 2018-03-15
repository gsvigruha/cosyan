package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;

public class InsertStatementWithRulesPerformanceTest extends UnitTestBase {

  private static final int N1 = 1000;
  private static final int N2 = 10000;

  @Test
  public void testInsertWithLookupTable() {
    execute("create table t1 (a varchar, b integer, constraint pk_a primary key (a));");
    for (int i = 0; i < N1; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    execute("create table t2 (a varchar, c integer, "
        + "constraint fk_a foreign key (a) references t1(a),"
        + "constraint c_1 check (fk_a.b = c));");
    long t = System.currentTimeMillis();
    for (int i = 0; i < N2; i++) {
      int j = i % N1;
      execute("insert into t2 values ('abc" + j + "' ," + j + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with rules inserted in " + t);
  }
}
