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
    assertEquals(new BasicColumn(0, DataTypes.StringType, false), tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, DataTypes.LongType, true), tableMeta.column(new Ident("b")));
    assertEquals(new BasicColumn(2, DataTypes.DoubleType, true), tableMeta.column(new Ident("c")));
    assertEquals(new BasicColumn(3, DataTypes.BoolType, true), tableMeta.column(new Ident("d")));
    assertEquals(new BasicColumn(4, DataTypes.DateType, true), tableMeta.column(new Ident("e")));
  }
}
