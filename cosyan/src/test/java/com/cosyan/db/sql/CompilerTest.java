package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.TableReader;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.tools.CSVConverter;
import com.google.common.collect.ImmutableMap;

public class CompilerTest {

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
    CSVConverter csvConverter = new CSVConverter(metaRepo);

    csvConverter.convertWithSchema(
        "table",
        "target/test-classes/simple.csv",
        ImmutableMap.of("a", DataTypes.StringType, "b", DataTypes.LongType, "c", DataTypes.DoubleType),
        Optional.empty(),
        Optional.empty());
  }

  @Test
  public void testReadFirstLine() throws Exception {
    SyntaxTree tree = parser.parse("select * from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "abc", "b", 1L, "c", 1.0), reader.read());
  }

  @Test
  public void testReadArithmeticExpressions1() throws Exception {
    SyntaxTree tree = parser.parse("select b + 2, c * 3.0, c / b, c - 1, 3 % 2 from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", 3.0, "_c2", 1.0, "_c3", 0.0, "_c4", 1L), reader.read());
  }

  @Test
  public void testReadArithmeticExpressions2() throws Exception {
    SyntaxTree tree = parser.parse("select a + 'xyz' from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", "abcxyz"), reader.read());
  }

  @Test
  public void testReadLogicExpressions1() throws Exception {
    SyntaxTree tree = parser.parse("select b = 1, b < 0.0, c > 0, c <= 1, c >= 2.0 from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", false), reader.read());
  }

  @Test
  public void testReadLogicExpressions2() throws Exception {
    SyntaxTree tree = parser.parse("select a = 'abc', a > 'b', a < 'x', a >= 'ab', a <= 'x' from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", true), reader.read());
  }

  @Test
  public void testReadStringFunction() throws Exception {
    SyntaxTree tree = parser.parse("select length(a), upper(a), substr(a, 1, 1) from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", "ABC", "_c2", "b"), reader.read());
  }

  @Test
  public void testWhere() throws Exception {
    SyntaxTree tree = parser.parse("select * from table where b > 1;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.read());
  }

  @Test
  public void testInnerSelect() throws Exception {
    SyntaxTree tree = parser.parse("select * from (select * from table where b > 1);");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.read());
  }

  @Test
  public void testAliasing() throws Exception {
    SyntaxTree tree = parser.parse("select b + 2 as x, c * 3.0 as y from table;");
    TableMeta tableMeta = compiler.query(tree);
    TableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("x", 3L, "y", 3.0), reader.read());
  }
}
