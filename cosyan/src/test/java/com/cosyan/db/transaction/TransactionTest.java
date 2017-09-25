package com.cosyan.db.transaction;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.sql.Result.QueryResult;

public class TransactionTest extends UnitTestBase {
  @Test
  public void testMultipleInserts() throws InterruptedException {
    execute("create table t1 (a integer, b float, c varchar);");
    execute("insert into t1 values(1, 1.0, 'abc');" +
        "insert into t1 values(1, 1.0, 'abc');" +
        "insert into t1 values(1, 1.0, 'abc');");
    QueryResult result = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { 1L, 1.0, "abc" },
        { 1L, 1.0, "abc" },
        { 1L, 1.0, "abc" }
    }, result);
  }
}
