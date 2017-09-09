package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class CreateStatementTest {

  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Compiler compiler;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    metaRepo = new MetaRepo(new Config(props));
    parser = new Parser();
    compiler = new Compiler(metaRepo);
  }

  @Test
  public void testCreateTable() throws Exception {
    SyntaxTree tree = parser.parse("create table t1 (a varchar not null, b integer, c float, d boolean, e timestamp);");
    compiler.statement(tree);
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
    SyntaxTree tree = parser.parse("create table t2 (a varchar unique not null, b integer unique);");
    compiler.statement(tree);
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t2"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, true),
        tableMeta.column(new Ident("b")));
  }

  @Test
  public void testCreateTablePrimaryKey() throws Exception {
    SyntaxTree tree = parser.parse("create table t3 (a varchar, constraint pk_a primary key (a));");
    compiler.statement(tree);
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t3"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true, true),
        tableMeta.column(new Ident("a")));
  }

  @Test
  public void testCreateTableForeignKey() throws Exception {
    compiler.statement(parser.parse("create table t4 (a varchar, constraint pk_a primary key (a));"));
    compiler.statement(parser.parse("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));"));
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
