package com.cosyan.db.lang.sql;

import java.util.Random;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.QueryResult;

public class SelectStatementPerformanceTest extends UnitTestBase {

  private static final int N = 20000;

  @Test
  public void testSelectWithWhereIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      QueryResult result = query("select * from t1 where a = 'abc" + r + "';");
      assertValues(new Object[][] { { "abc" + r, (long) r } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index queried in " + t + " " + speed(t, N));
  }

  @Test
  public void testSelectWithWhereNotIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      QueryResult result = query("select * from t2 where a = 'abc" + r + "';");
      assertValues(new Object[][] { { "abc" + r, (long) r } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index queried in " + t + " " + speed(t, N));
  }
}
