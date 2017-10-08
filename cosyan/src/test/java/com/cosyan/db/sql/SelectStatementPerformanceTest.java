package com.cosyan.db.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.sql.Result.QueryResult;

public class SelectStatementPerformanceTest extends UnitTestBase {

  @Test
  public void testSelectWithWhereNonIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < 10000; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records inserted in " + t);
    t = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      QueryResult result = query("select * from t1 where a = 'abc" + i + "';");
      assertValues(new Object[][] { { "abc" + i, (long) i } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records queried in " + t);
  }

  @Test
  public void testSelectWithWhereIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < 10000; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records inserted in " + t);
    t = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      QueryResult result = query("select * from t2 where a = 'abc" + i + "';");
      assertValues(new Object[][] { { "abc" + i, (long) i } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records queried in " + t);
  }
}
