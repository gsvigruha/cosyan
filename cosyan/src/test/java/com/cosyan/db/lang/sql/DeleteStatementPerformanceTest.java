package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.StatementResult;

public class DeleteStatementPerformanceTest extends UnitTestBase {

  private static final int N = 2000;

  @Test
  public void testSelectWithWhereIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index inserted in " + t);
    t = System.currentTimeMillis();
    Random random = new Random();
    int deleted = 0;
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      StatementResult result = stmt("delete from t1 where a = 'abc" + r + "';");
      deleted += result.getAffectedLines();
    }
    assertTrue(deleted > N / 2);
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index queried in " + t);
  }

  @Test
  public void testSelectWithWhereNotIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index inserted in " + t);
    t = System.currentTimeMillis();
    Random random = new Random();
    int deleted = 0;
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      StatementResult result = stmt("delete from t2 where a = 'abc" + r + "';");
      deleted += result.getAffectedLines();
    }
    assertTrue(deleted > N / 2);
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index queried in " + t);
  }
}
