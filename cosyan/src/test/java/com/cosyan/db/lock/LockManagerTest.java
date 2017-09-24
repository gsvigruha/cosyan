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
        for (int i = 0; i < 100; i++) {
          s.execute("update t1 set a = a + 1;");
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
}
