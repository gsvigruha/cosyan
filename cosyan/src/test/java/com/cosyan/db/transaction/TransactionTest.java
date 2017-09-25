package com.cosyan.db.transaction;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class TransactionTest extends UnitTestBase {
  @Test
  public void testMultipleInserts() throws InterruptedException {
    execute("create table t1 (a integer, b float, c varchar);");
    execute("insert into t1 values(1, 1.0, 'abc');" +
        "insert into t1 values(2, 1.0, 'abc');" +
        "insert into t1 values(3, 1.0, 'abc');");
    QueryResult result = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { 1L, 1.0, "abc" },
        { 2L, 1.0, "abc" },
        { 3L, 1.0, "abc" }
    }, result);
  }

  @Test
  public void testMultipleInsertsWithIndex() throws InterruptedException, ModelException, IOException {
    execute("create table t2 (a integer, b float, c varchar, constraint pk_a primary key (a));");
    execute("insert into t2 values(1, 1.0, 'abc');" +
        "insert into t2 values(2, 1.0, 'abc');" +
        "insert into t2 values(3, 1.0, 'abc');");
    QueryResult result = query("select * from t2;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { 1L, 1.0, "abc" },
        { 2L, 1.0, "abc" },
        { 3L, 1.0, "abc" }
    }, result);

    TableIndex t2a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t2"))).get("a");
    assertEquals(0L, t2a.get(1L));
    assertEquals(8L, t2a.get(2L));
    assertEquals(16L, t2a.get(3L));
  }
}
