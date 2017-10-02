package com.cosyan.db.transaction;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.Result.TransactionResult;
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
    // Record length: 1 + (1 + 8) + (1 + 8) + (1 + 4 + 6) = 30.
    assertEquals(0L, t2a.get(1L));
    assertEquals(30L, t2a.get(2L));
    assertEquals(60L, t2a.get(3L));
  }

  @Test
  public void testInsertAndSelect() throws InterruptedException, ModelException, IOException {
    execute("create table t3 (a varchar);");
    TransactionResult result = transaction("insert into t3 values('a');" +
        "select * from t3;" +
        "insert into t3 values('b');" +
        "select * from t3;");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(1));
    assertValues(new Object[][] { { "a" } }, (QueryResult) result.getResults().get(1));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] {
        { "a" },
        { "b" } }, (QueryResult) result.getResults().get(3));

    QueryResult resultAfterCommit = query("select * from t3;");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] {
        { "a" },
        { "b" } }, resultAfterCommit);
  }

  @Test
  public void testInsertAndDelete() throws InterruptedException, ModelException, IOException {
    execute("create table t4 (a varchar);");
    TransactionResult result = transaction("insert into t4 values('a');" +
        "insert into t4 values('b');" +
        "delete from t4 where a = 'a';" +
        "select * from t4;" +
        "insert into t4 values('c');" +
        "delete from t4 where a = 'c';" +
        "select * from t4;");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] { { "b" } }, (QueryResult) result.getResults().get(3));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(6));
    assertValues(new Object[][] { { "b" } }, (QueryResult) result.getResults().get(6));

    QueryResult resultAfterCommit = query("select * from t4;");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] { { "b" } }, resultAfterCommit);
  }
}
