package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class CreateStatementTest extends UnitTestBase {

  @Test
  public void testCreateTable() throws Exception {
    execute("create table t1 (a varchar not null, b integer, c float, d boolean, e timestamp);");
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t1"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, false),
        tableMeta.column(new Ident("b")));
    assertEquals(new BasicColumn(2, "c", DataTypes.DoubleType, true, false),
        tableMeta.column(new Ident("c")));
    assertEquals(new BasicColumn(3, "d", DataTypes.BoolType, true, false),
        tableMeta.column(new Ident("d")));
    assertEquals(new BasicColumn(4, "e", DataTypes.DateType, true, false),
        tableMeta.column(new Ident("e")));
  }

  @Test
  public void testCreateTableUniqueColumns() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer unique);");
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t2"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, true),
        tableMeta.column(new Ident("b")));
  }

  @Test
  public void testCreateTablePrimaryKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t3"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true, true),
        tableMeta.column(new Ident("a")));
  }

  @Test
  public void testCreateTableForeignKey() throws Exception {
    execute("create table t4 (a varchar, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));");
    MaterializedTableMeta t5 = metaRepo.table(new Ident("t5"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, true, false, false),
        t5.column(new Ident("a")));
    assertEquals(new BasicColumn(1, "b", DataTypes.StringType, true, false, true),
        t5.column(new Ident("b")));
    MaterializedTableMeta t4 = metaRepo.table(new Ident("t4"));
    assertEquals(ImmutableMap.of("fk_b", new ForeignKey(
        "fk_b",
        (BasicColumn) t5.column(new Ident("b")),
        t4,
        (BasicColumn) t4.column(new Ident("a")))),
        t5.getForeignKeys());
    assertEquals(ImmutableMap.of("fk_b", new ForeignKey(
        "fk_b",
        (BasicColumn) t4.column(new Ident("a")),
        t5,
        (BasicColumn) t5.column(new Ident("b")))),
        t4.getReverseForeignKeys());
  }
}
