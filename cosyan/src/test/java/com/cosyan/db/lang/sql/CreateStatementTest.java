package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.SourceValues;
import com.google.common.collect.ImmutableMap;

public class CreateStatementTest extends UnitTestBase {

  @Test
  public void testCreateTable() throws Exception {
    execute("create table t1 (a varchar not null, b integer, c float, d boolean, e timestamp);");
    MaterializedTableMeta tableMeta = metaRepo.table(new Ident("t1"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, false),
        tableMeta.column(new Ident("a")).getMeta());
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, false),
        tableMeta.column(new Ident("b")).getMeta());
    assertEquals(new BasicColumn(2, "c", DataTypes.DoubleType, true, false),
        tableMeta.column(new Ident("c")).getMeta());
    assertEquals(new BasicColumn(3, "d", DataTypes.BoolType, true, false),
        tableMeta.column(new Ident("d")).getMeta());
    assertEquals(new BasicColumn(4, "e", DataTypes.DateType, true, false),
        tableMeta.column(new Ident("e")).getMeta());
  }

  @Test
  public void testCreateTableUniqueColumns() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer unique);");
    MaterializedTableMeta tableMeta = metaRepo.table(new Ident("t2"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true),
        tableMeta.column(new Ident("a")).getMeta());
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, true),
        tableMeta.column(new Ident("b")).getMeta());
  }

  @Test
  public void testCreateTablePrimaryKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    MaterializedTableMeta tableMeta = metaRepo.table(new Ident("t3"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true, true),
        tableMeta.column(new Ident("a")).getMeta());
  }

  @Test
  public void testCreateTableForeignKey() throws Exception {
    execute("create table t4 (a varchar, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));");
    MaterializedTableMeta t5 = metaRepo.table(new Ident("t5"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, true, false, false),
        t5.column(new Ident("a")).getMeta());
    assertEquals(new BasicColumn(1, "b", DataTypes.StringType, true, false, true),
        t5.column(new Ident("b")).getMeta());
    MaterializedTableMeta t4 = metaRepo.table(new Ident("t4"));
    assertEquals(ImmutableMap.of("fk_b", new ForeignKey(
        "fk_b",
        "rev_fk_b",
        t5,
        (BasicColumn) t5.column(new Ident("b")).getMeta(),
        t4,
        (BasicColumn) t4.column(new Ident("a")).getMeta())),
        t5.foreignKeys());
    assertEquals(ImmutableMap.of("rev_fk_b", new ReverseForeignKey(
        "rev_fk_b",
        "fk_b",
        t4,
        (BasicColumn) t4.column(new Ident("a")).getMeta(),
        t5,
        (BasicColumn) t5.column(new Ident("b")).getMeta())),
        t4.reverseForeignKeys());
  }

  @Test
  public void testCreateTableAlreadyExists() throws Exception {
    execute("create table t6 (a varchar);");
    ErrorResult error = error("create table t6 (a varchar);");
    assertEquals("Table 't6' already exists.", error.getError().getMessage());
  }

  @Test
  public void testCreateIndex() throws Exception {
    execute("create table t7 (a varchar);");
    execute("create index t7.a;");
    assertEquals(1, metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t7"))).size());
  }

  @Test
  public void testCreateIndexErrors() throws Exception {
    execute("create table t8 (a varchar unique);");
    ErrorResult error = error("create index t8.a;");
    assertEquals("Cannot create index on 't8.a', column is already indexed.", error.getError().getMessage());
  }

  @Test
  public void testCreateSimpleRule() throws Exception {
    execute("create table t9 (a integer, constraint c_a check (a > 1));");
    Rule rule = metaRepo.table(new Ident("t9")).rules().get("c_a");
    assertEquals("c_a", rule.getName());
    assertEquals(false, rule.getColumn().getValue(SourceValues.of(new Object[] { 0L })));
    assertEquals(true, rule.getColumn().getValue(SourceValues.of(new Object[] { 2L })));
  }

  @Test
  public void testCreateReferencingRule() throws Exception {
    execute("create table t10 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t11 (a varchar,"
        + "constraint fk_a foreign key (a) references t10(a),"
        + "constraint c_b check (fk_a.b > 1));");
    MaterializedTableMeta t11 = metaRepo.table(new Ident("t11"));
    Rule rule = t11.rules().get("c_b");
    assertEquals("c_b", rule.getName());

    assertEquals(1, t11.ruleDependencies().size());
    assertEquals(0, t11.reverseRuleDependencies().getColumnDeps().size());
    assertEquals("fk_a", t11.ruleDependencies().get("fk_a").getForeignKey().getName());
    assertEquals("b", t11.ruleDependencies().get("fk_a").getColumnDeps().get("b").getName());

    MaterializedTableMeta t10 = metaRepo.table(new Ident("t10"));
    assertEquals(0, t10.ruleDependencies().size());
    assertEquals(1, t10.reverseRuleDependencies().getColumnDeps().size());
    assertEquals(1, t10.reverseRuleDependencies().getColumnDeps().get("b").size());
    assertEquals("rev_fk_a", t10.reverseRuleDependencies().getColumnDeps().get("b").get("rev_fk_a").getForeignKey().getName());
    assertEquals(1, t10.reverseRuleDependencies().getColumnDeps().get("b").get("rev_fk_a").getRules().size());
    assertEquals("c_b", t10.reverseRuleDependencies().getColumnDeps().get("b").get("rev_fk_a").getRules().get("c_b").getName());
  }

  @Test
  public void testCreateReferencingRuleMultipleLevel() throws Exception {
    execute("create table t12 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t13 (c varchar, d varchar, constraint pk_c primary key (c), "
        + "constraint fk_d foreign key (d) references t12(a))");
    execute("create table t14 (e varchar, constraint fk_e foreign key (e) references t13(c), "
        + "constraint c_b check (fk_e.fk_d.b > 1));");

    MaterializedTableMeta t12 = metaRepo.table(new Ident("t12"));
    assertEquals(0, t12.ruleDependencies().size());
    assertEquals(1, t12.reverseRuleDependencies().getColumnDeps().size());

    MaterializedTableMeta t13 = metaRepo.table(new Ident("t13"));
    assertEquals(0, t13.ruleDependencies().size());
    assertEquals(0, t13.reverseRuleDependencies().getColumnDeps().size());

    MaterializedTableMeta t14 = metaRepo.table(new Ident("t14"));
    assertEquals(1, t14.ruleDependencies().size());
    assertEquals(0, t14.reverseRuleDependencies().getColumnDeps().size());
  }
}
