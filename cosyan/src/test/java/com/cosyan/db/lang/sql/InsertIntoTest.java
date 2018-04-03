package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;

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
    assertEquals(16L, t9a.get("y")[0]);
    TableMultiIndex t10b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t10"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L, 27L }, t10b.get("x"));
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

  @Test
  public void testRuleReferencingOtherTable() throws Exception {
    execute("create table t17 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t18 (a varchar, "
        + "constraint fk_a foreign key (a) references t17(a), "
        + "constraint c_a check (fk_a.b = 1));");
    execute("insert into t17 values ('x', 1), ('y', 2);");
    execute("insert into t18 values ('x');");
    {
      QueryResult result = query("select a, fk_a.a as a2, fk_a.b as b2 from t18;");
      assertHeader(new String[] { "a", "a2", "b2" }, result);
      assertValues(new Object[][] { { "x", "x", 1L } }, result);
    }
    {
      ErrorResult result = error("insert into t18 values ('y');");
      assertError(RuleException.class, "Constraint check c_a failed.", result);
    }
  }

  @Test
  public void testRuleReferencingOtherMultiTable() throws Exception {
    execute("create table t19 (a varchar, constraint pk_a primary key (a));");
    execute("create table t20 (a varchar, constraint fk_a foreign key (a) references t19(a));");
    execute("alter table t19 add ref s (select count(a) as c from rev_fk_a);");
    execute("alter table t19 add constraint c_1 check (s.c <= 2);");

    execute("insert into t19 values ('x');");
    execute("insert into t20 values ('x'), ('x');");

    {
      QueryResult result = query("select fk_a.a from t20;");
      assertHeader(new String[] { "a" }, result);
      assertValues(new Object[][] { { "x" }, { "x" } }, result);
    }
    {
      ErrorResult result = error("insert into t20 values ('x');");
      assertError(RuleException.class, "Referencing constraint check t19.c_1 failed.", result);
    }
  }

  @Test
  public void testRuleReferencingOtherMultiTable_MultipleLevels() throws Exception {
    execute("create table t21 (a varchar, constraint pk_a primary key (a));");
    execute("create table t22 (b varchar, c varchar, "
        + "constraint pk_b primary key (b), "
        + "constraint fk_c foreign key (c) references t21(a));");
    execute("create table t23 (d varchar, e integer, constraint fk_d foreign key (d) references t22(b));");
    execute("alter table t22 add ref s (select sum(e) as se from rev_fk_d);");
    execute("alter table t21 add ref s (select sum(s.se) as sse from rev_fk_c);");
    execute("alter table t21 add constraint c_c check (s.sse <= 1);");

    execute("insert into t21 values ('x');");
    execute("insert into t22 values ('a', 'x');");
    execute("insert into t23 values ('a', 1);");

    QueryResult r1 = query("select e, fk_d.fk_c.a from t23;");
    assertValues(new Object[][] { { 1L, "x" } }, r1);

    ErrorResult e1 = error("insert into t23 values ('a', 1);");
    assertEquals("Referencing constraint check t21.c_c failed.", e1.getError().getMessage());
    QueryResult r2 = query("select e, fk_d.fk_c.a from t23;");
    assertValues(new Object[][] { { 1L, "x" } }, r2);
  }

  @Test
  public void testRuleReferencingOtherMultiTable_GlobalTableDependency() throws Exception {
    execute("create table t24 (a varchar, constraint pk_a primary key (a));");
    execute("create table t25 (a varchar, constraint fk_a foreign key (a) references t24(a));");
    // The rule does not reference any columns.
    execute("alter table t24 add ref s (select count(1) as c from rev_fk_a);");
    execute("alter table t24 add constraint c_1 check (s.c <= 2);");

    execute("insert into t24 values ('x');");
    execute("insert into t25 values ('x'), ('x');");

    {
      QueryResult result = query("select fk_a.a from t25;");
      assertHeader(new String[] { "a" }, result);
      assertValues(new Object[][] { { "x" }, { "x" } }, result);
    }
    {
      ErrorResult result = error("insert into t25 values ('x');");
      assertError(RuleException.class, "Referencing constraint check t24.c_1 failed.", result);
    }
  }

  @Test
  public void testRuleReferencingOtherMultiTable_Where() throws Exception {
    execute("create table t26 (a varchar, constraint pk_a primary key (a));");
    execute("create table t27 (a varchar, b integer, c integer, "
        + "constraint fk_a foreign key (a) references t26(a));");
    execute("alter table t26 add ref s (select sum(b) as sb from rev_fk_a where c = 2);");
    execute("alter table t26 add constraint c_1 check (s.sb <= 2);");

    execute("insert into t26 values ('x');");
    execute("insert into t27 values ('x', 2, 2), ('x', 2, 1);");

    {
      QueryResult result = query("select fk_a.a, b, c from t27;");
      assertHeader(new String[] { "a", "b", "c" }, result);
      assertValues(new Object[][] { { "x", 2L, 2L }, { "x", 2L, 1L } }, result);
    }
    {
      ErrorResult result = error("insert into t27 values ('x', 1, 2);");
      assertError(RuleException.class, "Referencing constraint check t26.c_1 failed.", result);
    }
  }

  @Test
  public void testNullableForeignKeyInRule() {
    execute("create table t28 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t29 (a2 varchar, b2 varchar,"
        + "constraint fk_a foreign key (a2) references t28(a1),"
        + "constraint c_1 check (fk_a.b1 > 0));");

    execute("insert into t28 values ('x', 1);");
    execute("insert into t29 values ('x', 'a');");
    execute("insert into t29 (b2) values ('b');");

    QueryResult r1 = query("select a2, b2, fk_a.b1 from t29;");
    assertHeader(new String[] { "a2", "b2", "b1" }, r1);
    assertValues(new Object[][] {
        { "x", "a", 1L },
        { DataTypes.NULL, "b", DataTypes.NULL } }, r1);
  }

  @Test
  public void testInsertNull() {
    execute("create table t30 (a1 varchar, b1 integer);");
    execute("insert into t30 values ('x', null);");

    QueryResult r1 = query("select a1, b1 from t30;");
    assertHeader(new String[] { "a1", "b1" }, r1);
    assertValues(new Object[][] { { "x", DataTypes.NULL } }, r1);
  }

  @Test
  public void testInsertWrongType() {
    execute("create table t31 (a varchar, b integer, c float, d timestamp, e boolean);");
    ErrorResult e1 = error("insert into t31 values (1, null, null, null, null);");
    assertError(RuleException.class, "Expected 'varchar' but got 'integer'.", e1);
    ErrorResult e2 = error("insert into t31 values (null, 'x', null, null, null);");
    assertError(RuleException.class, "Expected 'integer' but got 'varchar'.", e2);
    ErrorResult e3 = error("insert into t31 values (null, null, 1, null, null);");
    assertError(RuleException.class, "Expected 'float' but got 'integer'.", e3);
    ErrorResult e4 = error("insert into t31 values (null, null,  null, 1, null);");
    assertError(RuleException.class, "Expected 'timestamp' but got 'integer'.", e4);
    ErrorResult e5 = error("insert into t31 values (null, null, null, null, 'x');");
    assertError(RuleException.class, "Expected 'boolean' but got 'varchar'.", e5);
  }

  @Test
  public void testLookupTable() {
    execute("create lookup table t32 (a varchar);");
    execute("insert into t32 values ('x'), ('y');");
    QueryResult r1 = query("select a from t32;");
    assertHeader(new String[] { "a" }, r1);
    assertValues(new Object[][] { { "x" }, { "y" } }, r1);
  }
}
