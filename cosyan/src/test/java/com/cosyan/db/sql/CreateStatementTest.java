package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class CreateStatementTest {

  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Compiler compiler;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
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
    SyntaxTree tree = parser.parse("create table t1 (a varchar unique not null, b integer unique);");
    compiler.statement(tree);
    ExposedTableMeta tableMeta = metaRepo.table(new Ident("t1"));
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, false, true),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, "b", DataTypes.LongType, true, true),
        tableMeta.column(new Ident("b")));
  }
}