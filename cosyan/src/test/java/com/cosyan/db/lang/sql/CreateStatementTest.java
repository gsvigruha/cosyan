package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.TableMultiIndex;
import com.google.common.collect.ImmutableMap;

public class CreateStatementTest extends UnitTestBase {

  @Test
  public void testCreateTable() throws Exception {
    execute("create table t1 (a varchar not null, b integer, c float, d boolean, e timestamp);");
    MaterializedTable tableMeta = metaRepo.table(new Ident("t1"));
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, false, false, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, new Ident("b"), DataTypes.LongType, true, false, false),
        tableMeta.column(new Ident("b")));
    assertEquals(new BasicColumn(2, new Ident("c"), DataTypes.DoubleType, true, false, false),
        tableMeta.column(new Ident("c")));
    assertEquals(new BasicColumn(3, new Ident("d"), DataTypes.BoolType, true, false, false),
        tableMeta.column(new Ident("d")));
    assertEquals(new BasicColumn(4, new Ident("e"), DataTypes.DateType, true, false, false),
        tableMeta.column(new Ident("e")));
  }

  @Test
  public void testCreateTableUniqueColumns() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer unique);");
    MaterializedTable tableMeta = metaRepo.table(new Ident("t2"));
    assertTrue(tableMeta.column(new Ident("a")).isUnique());
    assertTrue(tableMeta.column(new Ident("a")).isIndexed());
    assertFalse(tableMeta.column(new Ident("a")).isNullable());
    assertTrue(tableMeta.column(new Ident("b")).isUnique());
    assertTrue(tableMeta.column(new Ident("b")).isIndexed());
    assertTrue(tableMeta.column(new Ident("b")).isNullable());
  }

  @Test
  public void testCreateTablePrimaryKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    MaterializedTable tableMeta = metaRepo.table(new Ident("t3"));
    assertTrue(tableMeta.column(new Ident("a")).isUnique());
    assertTrue(tableMeta.column(new Ident("a")).isIndexed());
  }

  @Test
  public void testCreateTableForeignKey() throws Exception {
    execute("create table t4 (a varchar, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));");
    MaterializedTable t5 = metaRepo.table(new Ident("t5"));
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, false),
        t5.column(new Ident("a")));
    assertFalse(t5.column(new Ident("b")).isUnique());
    assertTrue(t5.column(new Ident("b")).isIndexed());
    MaterializedTable t4 = metaRepo.table(new Ident("t4"));
    assertEquals(ImmutableMap.of("fk_b", new ForeignKey(
        "fk_b",
        "rev_fk_b",
        t5,
        (BasicColumn) t5.column(new Ident("b")),
        t4,
        (BasicColumn) t4.column(new Ident("a")))),
        t5.foreignKeys());
    assertEquals(ImmutableMap.of("rev_fk_b", new ReverseForeignKey(
        "rev_fk_b",
        "fk_b",
        t4,
        (BasicColumn) t4.column(new Ident("a")),
        t5,
        (BasicColumn) t5.column(new Ident("b")))),
        t4.reverseForeignKeys());
  }

  @Test
  public void testCreateTableAlreadyExists() throws Exception {
    execute("create table t6 (a varchar);");
    ErrorResult error = error("create table t6 (a varchar);");
    assertEquals("[13, 15]: Table 't6' already exists.", error.getError().getMessage());
  }

  @Test
  public void testCreateIndex() throws Exception {
    execute("create table t7 (a varchar);");
    execute("create index t7.a;");
    assertEquals(1, metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t7"))).size());
  }

  @Test
  public void testCreateSimpleRule() throws Exception {
    execute("create table t9 (a integer, constraint c_a check (a > 1));");
    Rule rule = metaRepo.table(new Ident("t9")).rules().get("c_a");
    assertEquals("c_a", rule.getName());
    // TODO test something
  }

  @Test
  public void testCreateReferencingRule() throws Exception {
    execute("create table t10 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t11 (a varchar,"
        + "constraint fk_a foreign key (a) references t10(a),"
        + "constraint c_b check (fk_a.b > 1));");
    MaterializedTable t11 = metaRepo.table(new Ident("t11"));
    Rule rule = t11.rules().get("c_b");
    assertEquals("c_b", rule.getName());

    assertEquals(1, t11.ruleDependencies().size());
    assertEquals(0, t11.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a", t11.ruleDependencies().get("fk_a").ref().getName());

    MaterializedTable t10 = metaRepo.table(new Ident("t10"));
    assertEquals(0, t10.ruleDependencies().size());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t10.reverseRuleDependencies().getDeps().get("rev_fk_a").getKey().getName());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().get("rev_fk_a").rules().size());
    assertEquals("c_b",
        t10.reverseRuleDependencies().getDeps().get("rev_fk_a").rule("c_b").getName());
  }

  @Test
  public void testCreateReferencingRuleMultipleLevel() throws Exception {
    execute("create table t12 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t13 (c varchar, d varchar, constraint pk_c primary key (c), "
        + "constraint fk_d foreign key (d) references t12(a));");
    execute("create table t14 (e varchar, constraint fk_e foreign key (e) references t13(c), "
        + "constraint c_b check (fk_e.fk_d.b > 1));");

    MaterializedTable t12 = metaRepo.table(new Ident("t12"));
    assertEquals(0, t12.ruleDependencies().size());
    assertEquals(1, t12.reverseRuleDependencies().getDeps().size());

    MaterializedTable t13 = metaRepo.table(new Ident("t13"));
    assertEquals(0, t13.ruleDependencies().size());
    assertEquals(1, t13.reverseRuleDependencies().getDeps().size());

    MaterializedTable t14 = metaRepo.table(new Ident("t14"));
    assertEquals(1, t14.ruleDependencies().size());
    assertEquals(0, t14.reverseRuleDependencies().getDeps().size());
  }

  @Test
  public void testCreateRefTableTableDeps() throws Exception {
    execute("create table t15 (a varchar, constraint pk_a primary key (a));");
    execute("create table t16 (b varchar, c integer, constraint fk_a foreign key (b) references t15(a));");
    execute("alter table t15 add ref s (select sum(c) as sb from rev_fk_a);");

    MaterializedTable t15 = metaRepo.table(new Ident("t15"));
    TableDependencies deps = t15.reader().table(new Ident("s")).column(new Ident("sb")).tableDependencies();
    assertEquals(1, deps.getDeps().size());
    assertEquals(0, deps.getDeps().get("rev_fk_a").size());
  }

  @Test
  public void testCreateRefTableTableDepsMultipleLevel() throws Exception {
    execute("create table t17 (a varchar, constraint pk_a primary key (a));");
    execute(
        "create table t18 (b varchar, constraint pk_a primary key (b), constraint fk_a foreign key (b) references t17(a));");
    execute("create table t19 (c varchar, d integer, constraint fk_b foreign key (c) references t18(b));");
    execute("alter table t18 add ref s (select sum(d) as sd from rev_fk_b);");
    execute("alter table t17 add ref s (select sum(s.sd) as ssd from rev_fk_a);");

    MaterializedTable t17 = metaRepo.table(new Ident("t17"));
    TableDependencies deps = t17.reader().table(new Ident("s")).column(new Ident("ssd")).tableDependencies();
    assertEquals(1, deps.getDeps().size());
    assertEquals(1, deps.getDeps().get("rev_fk_a").size());
    assertEquals(0, deps.getDeps().get("rev_fk_a").dep("rev_fk_b").size());
  }

  @Test
  public void testCreateTableImmutableColumn() throws Exception {
    execute("create table t20 (a varchar immutable);");
    MaterializedTable t20 = metaRepo.table(new Ident("t20"));
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, true),
        t20.column(new Ident("a")));
  }

  @Test
  public void testCreateTableIDType() throws Exception {
    execute("create table t21 (a id);");
    MaterializedTable t21 = metaRepo.table(new Ident("t21"));
    assertTrue(t21.column(new Ident("a")).isUnique());
    assertTrue(t21.column(new Ident("a")).isIndexed());
    assertFalse(t21.column(new Ident("a")).isNullable());
    assertEquals(DataTypes.IDType, t21.column(new Ident("a")).getType());
  }

  @Test
  public void testCreateTableIDTypeErrors() throws Exception {
    ErrorResult e1 = error("create table t22 (a integer, b id);");
    assertEquals("[29, 30]: The ID column 'b' has to be the first one.", e1.getError().getMessage());
    ErrorResult e2 = error("create table t22 (a id, b integer, constraint pk_b primary key (b));");
    assertEquals("[46, 50]: There can only be one primary key.", e2.getError().getMessage());
  }

  @Test
  public void testCreateTableErrorNoReverseForeignKey() throws Exception {
    execute("create table t23 (a id);");
    ErrorResult e1 = error("create table t24 (a integer, "
        + "constraint fk_a foreign key (a) references t23(a), "
        + "constraint c check (b > 1));");
    assertEquals("[100, 101]: Column 'b' not found in table 't24'.", e1.getError().getMessage());
    MaterializedTable t23 = metaRepo.table(new Ident("t23"));
    assertEquals(0, t23.reverseForeignKeys().size());
    assertFalse(metaRepo.hasTable("t24"));
  }

  @Test
  public void testCreateTableCheckNotBoolean() throws Exception {
    ErrorResult e1 = error("create table t25 (a varchar, constraint c check (a.substr(1, 2)));");
    assertEquals("[40, 41]: Constraint check expression has to return a 'boolean': 'a.substr(1, 2)'.",
        e1.getError().getMessage());
  }

  @Test
  public void testCreateIndexWithData() throws Exception {
    execute("create table t26 (a varchar);");
    execute("insert into t26 values ('x'), ('y'), ('x');");
    execute("create index t26.a;");
    MaterializedTable t26 = metaRepo.table(new Ident("t26"));
    assertTrue(t26.column(new Ident("a")).isIndexed());
    TableMultiIndex index = metaRepo.collectMultiIndexes(t26).get("a");
    assertArrayEquals(new long[] { 0L, 32L }, index.get("x"));
    assertArrayEquals(new long[] { 16L }, index.get("y"));
  }
}
