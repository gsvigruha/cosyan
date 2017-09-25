package com.cosyan.db.lock;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.session.Session;
import com.cosyan.db.sql.Result.QueryResult;

public class LockManagerTest extends UnitTestBase {

  private Thread runXTimes(String sql, int x) {
    return new Thread() {
      public void run() {
        Session s = dbApi.getSession();
        for (int i = 0; i < x; i++) {
          s.execute(sql.replace("$x", String.valueOf(i)));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
  }

  @Test
  public void testParallelUpdateOfOneTable() throws InterruptedException {
    Session s = dbApi.getSession();
    s.execute("create table t1 (a integer);");
    s.execute("insert into t1 values (1);");
    {
      QueryResult result = query("select * from t1;", s);
      assertHeader(new String[] { "a" }, result);
      assertValues(new Object[][] { { 1L } }, result);
    }

    Thread t1 = runXTimes("update t1 set a = a + 1;", 100);
    Thread t2 = runXTimes("update t1 set a = a + 1;", 100);
    Thread t3 = runXTimes("update t1 set a = a + 1;", 100);

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    QueryResult result = query("select * from t1;", s);
    assertHeader(new String[] { "a" }, result);
    assertValues(new Object[][] { { 301L } }, result);
  }

  @Test
  public void testParallelDeletesOfOneTable() throws InterruptedException {
    Session s = dbApi.getSession();
    s.execute("create table t2 (a integer, b float, c varchar);");
    for (int i = 0; i < 100; i++) {
      s.execute("insert into t2 values (" + i + ", " + i + ".0, '" + i + "');");
    }
    {
      QueryResult result = query("select count(1) from t2;", s);
      assertValues(new Object[][] { { 100L } }, result);
    }

    Thread t1 = runXTimes("delete from t2 where a = $x * 4;", 25);
    Thread t2 = runXTimes("delete from t2 where a = $x * 4 + 1;", 25);
    Thread t3 = runXTimes("delete from t2 where a = $x * 4 + 2;", 25);

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    {
      QueryResult result = query("select count(1) from t2;", s);
      assertValues(new Object[][] { { 25L } }, result);
    }
    {
      QueryResult result = query("select count(1) from t2 where a % 4 = 3;", s);
      assertValues(new Object[][] { { 25L } }, result);
    }
  }

  @Test
  public void testParallelSelectAndInserts() throws InterruptedException {
    Session s = dbApi.getSession();
    s.execute("create table t3 (a integer, b varchar);");
    s.execute("create table t4 (x integer);");

    Thread t1 = runXTimes("insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');", 25);

    Thread t2 = runXTimes("insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');" +
        "insert into t3 values ($x, 'abc');", 25);

    Thread t3 = new Thread() {
      public void run() {
        Session s = dbApi.getSession();
        for (int i = 0; i < 25; i++) {
          QueryResult result = query("select count(1) from t3;", s);
          long cnt = (Long) result.getValues().get(0).get(0);
          s.execute("insert into t4 values (" + cnt + ");");
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    {
      QueryResult result = query("select count(1) from t4 where x % 4 = 0;", s);
      assertValues(new Object[][] { { 25L } }, result);
    }
    {
      QueryResult result = query("select min(x), max(x) from t4;", s);
      System.out.println("  min, max: " + result.getValues().get(0));
      assert((Long) result.getValues().get(0).get(0) < (Long) result.getValues().get(0).get(1));
    }
  }
}
