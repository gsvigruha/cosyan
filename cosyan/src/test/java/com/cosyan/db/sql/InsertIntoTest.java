package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo.RuleException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class InsertIntoTest extends UnitTestBase {

  @Test
  public void testInsertIntoTable() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("insert into t1 values ('x', 1, 2.0);");
    QueryResult result = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] { { "x", 1L, 2.0 } }, result);
  }

  @Test
  public void testInsertNullsIntoTable() throws Exception {
    execute("create table t2 (a varchar, b integer, c float);");
    execute("insert into t2 values ('x', 1, 2.0);");
    execute("insert into t2 (a, c) values ('y', 3.0);");
    execute("insert into t2 (c, b) values (4.0, 5);");
    QueryResult result = query("select * from t2;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", DataTypes.NULL, 3.0 },
        { DataTypes.NULL, 5L, 4.0 } }, result);
  }

  @Test
  public void testNotNullable() throws Exception {
    execute("create table t3 (a varchar not null, b integer);");
    execute("insert into t3 values ('x', 1);");
    ErrorResult result = error("insert into t3 (b) values (2);");
    assertError(RuleException.class, "Column is not nullable (mandatory).", result);
  }

  @Test
  public void testUniqueNotNull() throws Exception {
    execute("create table t4 (a varchar unique not null);");
    execute("insert into t4 values ('x');");
    ErrorResult result = error("insert into t4 values ('x');");
    assertError(RuleException.class, "Key 'x' already present in index.", result);
  }

  @Test
  public void testUniqueNull() throws Exception {
    execute("create table t5 (a varchar unique, b integer);");
    execute("insert into t5 values ('x', 1);");
    execute("insert into t5 (b) values (1);");
    execute("insert into t5 (b) values (1);");
    QueryResult result = query("select * from t5;");
    assertHeader(new String[] { "a", "b" }, result);
    assertValues(new Object[][] {
        { "x", 1L },
        { DataTypes.NULL, 1L },
        { DataTypes.NULL, 1L } }, result);
  }

  @Test
  public void testConstraint() throws Exception {
    execute("create table t6 (a integer, b integer, constraint c check(a + b > 1));");
    execute("insert into t6 values (1, 1);");
    ErrorResult result = error("insert into t6 values (0, 0);");
    assertError(RuleException.class, "Constraint check c failed.", result);
  }

  @Test
  public void testForeignKeys() throws Exception {
    execute("create table t7 (a varchar, constraint pk_a primary key (a));");
    execute("create table t8 (a varchar, b varchar, constraint fk_b foreign key (b) references t7(a));");
    ErrorResult r1 = error("insert into t8 values ('123', 'x');");
    assertError(RuleException.class, "Foreign key violation, value 'x' not present.", r1);

    execute("insert into t7 values ('x');");
    execute("insert into t8 values ('123', 'x');");
    QueryResult r2 = query("select * from t8;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] { { "123", "x" } }, r2);
  }

  @Test
  public void testForeignKeysIndexes() throws Exception {
    execute("create table t9 (a varchar, constraint pk_a primary key (a));");
    execute("create table t10 (a varchar, b varchar, constraint fk_b foreign key (b) references t9(a));");
    execute("insert into t9 values ('x');");
    execute("insert into t9 values ('y');");
    execute("insert into t10 values ('123', 'x');");
    execute("insert into t10 values ('456', 'x');");
    {
      QueryResult result = query("select * from t9;");
      assertHeader(new String[] { "a" }, result);
      assertValues(new Object[][] { { "x" }, { "y" } }, result);
    }
    {
      QueryResult result = query("select * from t10;");
      assertHeader(new String[] { "a", "b" }, result);
      assertValues(new Object[][] { { "123", "x" }, { "456", "x" } }, result);
    }
    TableIndex t9a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t9"))).get("a");
    assertEquals(0L, t9a.get("x")[0]);
    assertEquals(8L, t9a.get("y")[0]);
    TableMultiIndex t10b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t10"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L, 19L }, t10b.get("x"));
  }

  @Test
  public void testInsertIntoTableMultipleRows() throws Exception {
    execute("create table t11 (a varchar, b integer, c float);");
    execute("insert into t11 values ('x', 1, 2.0), ('y', 3, 4.0);");
    QueryResult result = query("select * from t11;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 } }, result);
  }

  @Test
  public void testReadFromEmptyTable() throws Exception {
    execute("create table t12 (a varchar);");
    QueryResult result = query("select * from t12;");
    assertHeader(new String[] { "a" }, result);
    assertValues(new Object[][] {}, result);
  }

  @Test
  public void testPrimaryKeys() throws Exception {
    execute("create table t13 (a varchar, constraint pk_a primary key (a));");
    execute("insert into t13 values ('x');");
    ErrorResult r1 = error("insert into t13 values ('x');");
    assertError(RuleException.class, "Key 'x' already present in index.", r1);

    execute("insert into t13 values ('y');");
    QueryResult r2 = query("select * from t13;");
    assertHeader(new String[] { "a" }, r2);
    assertValues(new Object[][] { { "x" }, { "y" } }, r2);
  }

  @Test
  public void testAggregateFromEmptyTable() throws Exception {
    execute("create table t14 (a varchar, b integer);");
    QueryResult result = query("select count(1) as c, max(a) as max, min(a) as min, sum(b) as s from t14;");
    assertHeader(new String[] { "c", "max", "min", "s" }, result);
    assertValues(new Object[][] { { 0L, DataTypes.NULL, DataTypes.NULL, DataTypes.NULL } }, result);
  }

  @Test
  public void testReverseForeignKeyIsUnique() throws Exception {
    execute("create table t15 (a varchar, constraint pk_a primary key (a));");
    execute("create table t16 (a varchar, "
        + "constraint pk_a primary key (a),"
        + "constraint fk_a foreign key (a) references t15(a));");
    execute("insert into t15 values ('x');");
    execute("insert into t16 values ('x');");
    QueryResult result = query("select * from t16;");
    assertHeader(new String[] { "a" }, result);
    assertValues(new Object[][] { { "x" } }, result);
  }
}
