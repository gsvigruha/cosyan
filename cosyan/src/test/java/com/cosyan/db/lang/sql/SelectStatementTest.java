package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.QueryResult;

public class SelectStatementTest extends UnitTestBase {

  @Test
  public void testValuesFromDependentTable() {
    execute("create table t1 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t2 (a2 varchar, b2 varchar, constraint fk_a foreign key (a2) references t1(a1));");
    execute("insert into t1 values ('x', 1), ('y', 2);");
    execute("insert into t2 values ('x', 'y'), ('x', 'z');");
    QueryResult result = query("select a2, b2, fk_a.a1, fk_a.b1 from t2;");
    assertHeader(new String[] { "a2", "b2", "a1", "b1" }, result);
    assertValues(new Object[][] {
        { "x", "y", "x", 1L },
        { "x", "z", "x", 1L } }, result);
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
}
