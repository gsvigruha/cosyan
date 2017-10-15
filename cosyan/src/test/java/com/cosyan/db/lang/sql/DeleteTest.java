package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;
import com.cosyan.db.lang.sql.SyntaxTree.Ident;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;

public class DeleteTest extends UnitTestBase {

  @Test
  public void testDeleteFromTable() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("insert into t1 values ('x', 1, 2.0);");
    execute("insert into t1 values ('y', 3, 4.0);");
    QueryResult r1 = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 }
    }, r1);

    execute("delete from t1 where a = 'x';");
    QueryResult r2 = query("select * from t1;");
    assertValues(new Object[][] {
        { "y", 3L, 4.0 }
    }, r2);
  }

  @Test
  public void testDeleteWithIndex() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer);");
    execute("insert into t2 values ('x', 1);");
    execute("insert into t2 values ('y', 2);");
    ErrorResult r1 = error("insert into t2 values ('x', 3);");
    assertError(RuleException.class, "Key 'x' already present in index.", r1);

    execute("delete from t2 where a = 'x';");
    execute("insert into t2 values ('x', 3);");

    QueryResult r2 = query("select * from t2;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] {
        { "y", 2L },
        { "x", 3L }
    }, r2);
  }

  @Test
  public void testDeleteWithForeignKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (a varchar, b varchar, constraint fk_b foreign key (b) references t3(a));");
    execute("insert into t3 values ('x');");
    execute("insert into t3 values ('y');");
    execute("insert into t4 values ('123', 'x');");

    ErrorResult r1 = error("delete from t3 where a = 'x';");
    assertError(RuleException.class, "Foreign key violation, key value 'x' has references.", r1);

    execute("delete from t4 where b = 'x';");
    execute("delete from t3 where a = 'x';");

    QueryResult r2 = query("select * from t3;");
    assertHeader(new String[] { "a" }, r2);
    assertValues(new Object[][] { { "y" } }, r2);
  }

  @Test
  public void testDeleteWithForeignKeyIndexes() throws Exception {
    execute("create table t5 (a varchar, constraint pk_a primary key (a));");
    execute("create table t6 (a varchar, b varchar, constraint fk_b foreign key (b) references t5(a));");
    execute("insert into t5 values ('x');");
    execute("insert into t5 values ('y');");
    execute("insert into t6 values ('123', 'x');");

    TableIndex t5a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t5"))).get("a");
    assertEquals(0L, t5a.get("x")[0]);
    assertEquals(16L, t5a.get("y")[0]);
    TableMultiIndex t6b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t6"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L }, t6b.get("x"));

    execute("delete from t6 where b = 'x';");
    execute("delete from t5 where a = 'x';");

    assertEquals(false, t5a.contains("x"));
    assertEquals(16L, t5a.get("y")[0]);
    org.junit.Assert.assertArrayEquals(new long[0], t6b.get("x"));
  }

  @Test
  public void testMultipleDeletes() throws Exception {
    execute("create table t7 (a varchar, b integer, c float);");
    execute("insert into t7 values ('x', 1, 2.0);");
    execute("insert into t7 values ('y', 3, 4.0);");
    execute("insert into t7 values ('z', 5, 6.0);");
    execute("insert into t7 values ('w', 7, 8.0);");
    QueryResult r1 = query("select * from t7;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 },
        { "z", 5L, 6.0 },
        { "w", 7L, 8.0 }
    }, r1);

    execute("delete from t7 where a = 'z';");
    execute("delete from t7 where b = 3;");
    execute("delete from t7 where a = 'x';");
    QueryResult r2 = query("select * from t7;");
    assertValues(new Object[][] {
        { "w", 7L, 8.0 }
    }, r2);
  }
}
