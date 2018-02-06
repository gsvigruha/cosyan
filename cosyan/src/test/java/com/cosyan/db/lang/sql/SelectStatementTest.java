package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;

public class SelectStatementTest extends UnitTestBase {

  @Test
  public void testValuesFromDependentTable() {
    execute("create table t1 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t2 (a2 varchar, b2 varchar, constraint fk_a foreign key (a2) references t1(a1));");
    execute("insert into t1 values ('x', 1), ('y', 2);");
    execute("insert into t2 values ('x', 'y'), ('x', 'z');");
    QueryResult r1 = query("select a2, b2, fk_a.a1, fk_a.b1 from t2;");
    assertHeader(new String[] { "a2", "b2", "a1", "b1" }, r1);
    assertValues(new Object[][] {
        { "x", "y", "x", 1L },
        { "x", "z", "x", 1L } }, r1);

    QueryResult r2 = query("select fk_a.a1.length() as a, fk_a.b1 + 1 as b from t2;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] {
        { 1L, 2L },
        { 1L, 2L } }, r2);
  }

  @Test
  public void testValuesFromDependentTableMultipleLevels() {
    execute("create table t3 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t4 (a2 varchar, b2 varchar, constraint pk_a primary key (a2), "
        + "constraint fk_b foreign key (b2) references t3(a1));");
    execute("create table t5 (a3 varchar, constraint fk_a foreign key (a3) references t4(a2));");
    execute("insert into t3 values ('x', 1);");
    execute("insert into t4 values ('a', 'x'), ('b', 'x');");
    execute("insert into t5 values ('a'), ('b');");
    QueryResult result = query("select a3, fk_a.a2, fk_a.b2, fk_a.fk_b.a1, fk_a.fk_b.b1 from t5;");
    assertHeader(new String[] { "a3", "a2", "b2", "a1", "b1" }, result);
    assertValues(new Object[][] {
        { "a", "a", "x", "x", 1L },
        { "b", "b", "x", "x", 1L } }, result);
  }

  @Test
  public void testMultipleForeignKeysToSameTable() {
    execute("create table t6 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t7 (a2 varchar, b2 varchar, "
        + "constraint fk_a foreign key (a2) references t6(a1), "
        + "constraint fk_b foreign key (b2) references t6(a1));");
    execute("insert into t6 values ('x', 1), ('y', 2);");
    execute("insert into t7 values ('x', 'y');");
    QueryResult result = query("select fk_a.a1, fk_a.b1, fk_b.a1 as a2, fk_b.b1 as b2 from t7;");
    assertHeader(new String[] { "a1", "b1", "a2", "b2" }, result);
    assertValues(new Object[][] { { "x", 1L, "y", 2L } }, result);
  }

  @Test
  public void testForeignKeysInAggregation() {
    execute("create table t8 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t9 (a2 varchar, constraint fk_a foreign key (a2) references t8(a1));");
    execute("insert into t8 values ('x', 1), ('y', 2);");
    execute("insert into t9 values ('x'), ('x'), ('x'), ('y');");

    QueryResult r1 = query("select a2, sum(fk_a.b1) as s from t9 group by a2;");
    assertHeader(new String[] { "a2", "s" }, r1);
    assertValues(new Object[][] { { "x", 3L }, { "y", 2L } }, r1);

    QueryResult r2 = query("select a1, sum(fk_a.b1) as s from t9 group by fk_a.a1 as a1;");
    assertHeader(new String[] { "a1", "s" }, r2);
    assertValues(new Object[][] { { "x", 3L }, { "y", 2L } }, r2);

    QueryResult r3 = query("select sum(fk_a.b1) as s from t9;");
    assertHeader(new String[] { "s" }, r3);
    assertValues(new Object[][] { { 5L } }, r3);

    QueryResult r4 = query("select a2, fk_a.b1.sum() as s from t9 group by a2;");
    assertHeader(new String[] { "a2", "s" }, r4);
    assertValues(new Object[][] { { "x", 3L }, { "y", 2L } }, r4);

    QueryResult r5 = query("select a1 from t9 group by fk_a.a1 as a1;");
    assertHeader(new String[] { "a1" }, r5);
    assertValues(new Object[][] { { "x" }, { "y" } }, r5);
  }

  @Test
  public void testReverseForeignKeys() {
    execute("create table t10 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t11 (a2 varchar, b2 integer, constraint fk_a foreign key (a2) references t10(a1));");
    execute("insert into t10 values ('x');");
    execute("insert into t11 values ('x', 1), ('x', 5);");

    QueryResult r1 = query("select a1, rev_fk_a.select(sum(b2)) from t10;");
    assertHeader(new String[] { "a1", "_c0" }, r1);
    assertValues(new Object[][] { { "x", 6L } }, r1);

    QueryResult r2 = query("select a1, rev_fk_a.select(b2.sum()) from t10;");
    assertHeader(new String[] { "a1", "_c0" }, r2);
    assertValues(new Object[][] { { "x", 6L } }, r2);

    QueryResult r3 = query("select a1 as a, rev_fk_a.select(sum(b2), count(b2)) from t10;");
    assertHeader(new String[] { "a", "_c0", "_c1" }, r3);
    assertValues(new Object[][] { { "x", 6L, 2L } }, r3);

    ErrorResult r4 = error("select a1 as a, rev_fk_a.select(b2) from t10;");
    assertEquals("Expected table or column.", r4.getError().getMessage());
  }

  @Test
  public void testReverseForeignKeyDependentTable() {
    execute("create table t12 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t14 (a4 varchar, b4 integer, constraint pk_a primary key (a4));");
    execute("create table t13 (a2 varchar, a3 varchar,"
        + "constraint fk_v foreign key (a2) references t12(a1),"
        + "constraint fk_w foreign key (a3) references t14(a4));");

    execute("insert into t12 values ('x'), ('y');");
    execute("insert into t14 values ('a', 1), ('b', 5);");
    execute("insert into t13 values ('x', 'a'), ('x', 'a'), ('y', 'b');");

    QueryResult r1 = query("select a1, rev_fk_v.select(sum(fk_w.b4)) from t12;");
    assertHeader(new String[] { "a1", "_c0" }, r1);
    assertValues(new Object[][] { { "x", 2L }, { "y", 5L } }, r1);
  }
}
