package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;

public class UpdateTest extends UnitTestBase {

  @Test
  public void testUpdateAllRecords() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("insert into t1 values ('x', 1, 2.0);");
    execute("insert into t1 values ('y', 3, 4.0);");
    QueryResult r1 = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 } }, r1);

    execute("update t1 set b = b + 10, c = c * 2;");
    QueryResult r2 = query("select * from t1;");
    assertValues(new Object[][] {
        { "x", 11L, 4.0 },
        { "y", 13L, 8.0 } }, r2);
  }

  @Test
  public void testUpdateWithWhere() throws Exception {
    execute("create table t2 (a varchar, b integer, c float);");
    execute("insert into t2 values ('x', 1, 2.0);");
    execute("insert into t2 values ('y', 3, 4.0);");
    QueryResult r1 = query("select * from t2;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 } }, r1);

    execute("update t2 set a = 'z' where a = 'x';");
    QueryResult r2 = query("select * from t2;");
    assertValues(new Object[][] {
        { "y", 3L, 4.0 },
        { "z", 1L, 2.0 } }, r2);
  }

  @Test
  public void testUpdateWithIndex() throws Exception {
    execute("create table t3 (a varchar unique not null, b integer);");
    execute("insert into t3 values ('x', 1);");
    execute("insert into t3 values ('y', 2);");

    ErrorResult r1 = error("update t3 set a = 'y' where a = 'x';");
    assertError(RuleException.class, "Key 'y' already present in index.", r1);

    execute("update t3 set a = 'z' where a = 'x';");
    QueryResult r2 = query("select * from t3;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] {
        { "y", 2L },
        { "z", 1L } }, r2);
  }

  @Test
  public void testUpdateWithForeignKey() throws Exception {
    execute("create table t4 (a varchar, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));");
    execute("insert into t4 values ('x');");
    execute("insert into t4 values ('y');");
    execute("insert into t5 values ('123', 'x');");

    ErrorResult r1 = error("update t4 set a = 'z' where a = 'x';");
    assertError(RuleException.class, "Foreign key violation, key value 'x' has references.", r1);

    ErrorResult r2 = error("update t5 set b = 'z' where b = 'x';");
    assertError(RuleException.class, "Foreign key violation, value 'z' not present.", r2);

    execute("update t5 set b = 'y' where b = 'x';");
    QueryResult r3 = query("select * from t5;");
    assertHeader(new String[] { "a", "b" }, r3);
    assertValues(new Object[][] { { "123", "y" } }, r3);

    statement("delete from t4 where a = 'x';");
  }

  @Test
  public void testUpdateWithForeignKeyIndexes() throws Exception {
    execute("create table t6 (a varchar, constraint pk_a primary key (a));");
    execute("create table t7 (a varchar, b varchar, constraint fk_b foreign key (b) references t6(a));");
    execute("insert into t6 values ('x');");
    execute("insert into t6 values ('y');");
    execute("insert into t7 values ('123', 'x');");

    TableIndex t6a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t6"))).get("a");
    assertEquals(0L, t6a.get("x")[0]);
    assertEquals(16L, t6a.get("y")[0]);
    TableMultiIndex t7b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t7"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L }, t7b.get("x"));
    assertEquals(false, t7b.contains("y"));

    execute("update t7 set b = 'y' where b = 'x';");
    assertEquals(0L, t6a.get("x")[0]);
    assertEquals(16L, t6a.get("y")[0]);
    assertEquals(false, t7b.contains("x"));
    org.junit.Assert.assertArrayEquals(new long[] { 27L }, t7b.get("y"));
  }

  @Test
  public void testUpdateMultipleTimes() throws Exception {
    execute("create table t8 (a integer, b float);");
    execute("insert into t8 values (1, 1.0);");
    QueryResult r1 = query("select * from t8;");
    assertHeader(new String[] { "a", "b" }, r1);
    assertValues(new Object[][] { { 1L, 1.0 } }, r1);

    for (int i = 0; i < 10; i++) {
      execute("update t8 set a = a + 1, b = b + 1.0;");
    }
    QueryResult r2 = query("select * from t8;");
    assertValues(new Object[][] { { 11L, 11.0 } }, r2);
  }

  @Test
  public void testUpdateReferencedByForeignKey() throws Exception {
    execute("create table t9 (a integer, b integer, constraint pk_a primary key (a));");
    execute("create table t10 (a integer, constraint fk_a foreign key (a) references t9(a));");
    execute("insert into t9 values (1, 1);");
    execute("insert into t10 values (1);");
    QueryResult r1 = query("select a, fk_a.a as a2, fk_a.b as b2 from t10;");
    assertHeader(new String[] { "a", "a2", "b2" }, r1);
    assertValues(new Object[][] { { 1L, 1L, 1L } }, r1);
    TableIndex t9a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t9"))).get("a");
    assertEquals(0L, t9a.get0(1L));
    TableMultiIndex t10a = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t10"))).get("a");
    assertEquals(0L, t10a.get(1L)[0]);

    execute("update t9 set b = 2;");
    QueryResult r2 = query("select a, fk_a.a as a2, fk_a.b as b2 from t10;");
    assertValues(new Object[][] { { 1L, 1L, 2L } }, r2);
    assertEquals(27L, t9a.get0(1L));
    assertEquals(0L, t10a.get(1L)[0]);
  }

  @Test
  public void testUpdateReferencedByRules() throws Exception {
    execute("create table t11 (a integer, b integer, constraint pk_a primary key (a));");
    execute("create table t12 (a integer, constraint fk_a foreign key (a) references t11(a),"
        + "constraint c_b check (fk_a.b > 0));");
    execute("insert into t11 values (1, 1);");
    execute("insert into t12 values (1);");

    QueryResult r1 = query("select a, fk_a.a as a2, fk_a.b as b2 from t12;");
    assertHeader(new String[] { "a", "a2", "b2" }, r1);
    assertValues(new Object[][] { { 1L, 1L, 1L } }, r1);

    execute("update t11 set b = 2;");
    QueryResult r2 = query("select a, fk_a.a as a2, fk_a.b as b2 from t12;");
    assertValues(new Object[][] { { 1L, 1L, 2L } }, r2);

    ErrorResult e = error("update t11 set b = 0;");
    assertEquals("Referencing constraint check t12.c_b failed.", e.getError().getMessage());
  }
}
