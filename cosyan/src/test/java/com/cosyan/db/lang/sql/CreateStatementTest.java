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
        (BasicColumn) t5.column(new Ident("b")).getMeta(),
        t4,
        (BasicColumn) t4.column(new Ident("a")).getMeta())),
        t5.foreignKeys());
    assertEquals(ImmutableMap.of("fk_b", new ReverseForeignKey(
        "fk_b",
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
    Rule rule = metaRepo.table(new Ident("t11")).rules().get("c_b");
    assertEquals("c_b", rule.getName());
    assertEquals(false, rule.getColumn().getValue(
        SourceValues.of(new Object[] { "x" }, ImmutableMap.of("fk_a", new Object[] { "x", 0L }))));
    assertEquals(true, rule.getColumn().getValue(
        SourceValues.of(new Object[] { "x" }, ImmutableMap.of("fk_a", new Object[] { "x", 2L }))));
  }
}
