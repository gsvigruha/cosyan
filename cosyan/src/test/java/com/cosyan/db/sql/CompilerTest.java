package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.IOTestUtil.DummyMaterializedTableMeta;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
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
    metaRepo.registerTable("table", new DummyMaterializedTableMeta(
        ImmutableMap.of(
            "a", new BasicColumn(0, DataTypes.StringType),
            "b", new BasicColumn(1, DataTypes.LongType),
            "c", new BasicColumn(2, DataTypes.DoubleType)),
        new Object[][] {
            new Object[] { "abc", 1L, 1.0 },
            new Object[] { "xyz", 5L, 6.7 } }));

    metaRepo.registerTable("large", new DummyMaterializedTableMeta(
        ImmutableMap.of(
            "a", new BasicColumn(0, DataTypes.StringType),
            "b", new BasicColumn(1, DataTypes.LongType),
            "c", new BasicColumn(2, DataTypes.DoubleType)),
        new Object[][] {
            new Object[] { "a", 1L, 2.0 },
            new Object[] { "a", 3L, 4.0 },
            new Object[] { "b", 5L, 6.0 },
            new Object[] { "b", 7L, 8.0 } }));
  }

  @Test
  public void testReadFirstLine() throws Exception {
    SyntaxTree tree = parser.parse("select * from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "abc", "b", 1L, "c", 1.0), reader.readColumns());
  }

  @Test
  public void testReadArithmeticExpressions1() throws Exception {
    SyntaxTree tree = parser.parse("select b + 2, c * 3.0, c / b, c - 1, 3 % 2 from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", 3.0, "_c2", 1.0, "_c3", 0.0, "_c4", 1L), reader.readColumns());
  }

  @Test
  public void testReadArithmeticExpressions2() throws Exception {
    SyntaxTree tree = parser.parse("select a + 'xyz' from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", "abcxyz"), reader.readColumns());
  }

  @Test
  public void testReadLogicExpressions1() throws Exception {
    SyntaxTree tree = parser.parse("select b = 1, b < 0.0, c > 0, c <= 1, c >= 2.0 from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", false),
        reader.readColumns());
  }

  @Test
  public void testReadLogicExpressions2() throws Exception {
    SyntaxTree tree = parser.parse("select a = 'abc', a > 'b', a < 'x', a >= 'ab', a <= 'x' from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", true),
        reader.readColumns());
  }

  @Test
  public void testReadStringFunction() throws Exception {
    SyntaxTree tree = parser.parse("select length(a), upper(a), substr(a, 1, 1) from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", "ABC", "_c2", "b"), reader.readColumns());
  }

  @Test
  public void testWhere() throws Exception {
    SyntaxTree tree = parser.parse("select * from table where b > 1;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.readColumns());
  }

  @Test
  public void testInnerSelect() throws Exception {
    SyntaxTree tree = parser.parse("select * from (select * from table where b > 1);");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.readColumns());
  }

  @Test
  public void testAliasing() throws Exception {
    SyntaxTree tree = parser.parse("select b + 2 as x, c * 3.0 as y from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("x", 3L, "y", 3.0), reader.readColumns());
  }

  @Test
  public void testGlobalAggregate() throws Exception {
    SyntaxTree tree = parser.parse("select sum(b) as b, sum(c) as c from large;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("b", 16L, "c", 20.0), reader.readColumns());
  }

  @Test
  public void testAggregatorsSum() throws Exception {
    SyntaxTree tree = parser.parse("select sum(1) as s1, sum(2.0) as s2, sum(b) as sb, sum(c) as sc from large;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("s1", 4L, "s2", 8.0, "sb", 16L, "sc", 20.0), reader.readColumns());
  }

  @Test
  public void testAggregatorsCount() throws Exception {
    SyntaxTree tree = parser.parse("select count(1) as c1, count(a) as ca, count(b) as cb, count(c) as cc from large;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("c1", 4L, "ca", 4L, "cb", 4L, "cc", 4L), reader.readColumns());
  }

  @Test
  public void testAggregatorsMax() throws Exception {
    SyntaxTree tree = parser.parse("select max(a) as a, max(b) as b, max(c) as c from large;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "b", "b", 7L, "c", 8.0), reader.readColumns());
  }

  @Test
  public void testAggregatorsMin() throws Exception {
    SyntaxTree tree = parser.parse("select min(a) as a, min(b) as b, min(c) as c from large;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "c", 2.0), reader.readColumns());
  }

  @Test
  public void testGroupBy() throws Exception {
    SyntaxTree tree = parser.parse("select a, sum(b) as b, sum(c) as c from large group by a;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 4L, "c", 6.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 12L, "c", 14.0), reader.readColumns());
  }

  @Test
  public void testExpressionInGroupBy() throws Exception {
    SyntaxTree tree = parser.parse("select a, sum(b + 1) as b, sum(c * 2.0) as c from large group by a;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 6L, "c", 12.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 14L, "c", 28.0), reader.readColumns());
  }

  @Test
  public void testExpressionFromGroupBy() throws Exception {
    SyntaxTree tree = parser.parse("select a, sum(b) + sum(c) as b from large group by a;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 10.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 26.0), reader.readColumns());
  }

  @Test
  public void testGroupByMultipleKey() throws Exception {
    SyntaxTree tree = parser.parse("select a, b, sum(c) as c from large group by a, b;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 5L, "c", 6.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L, "c", 8.0), reader.readColumns());
  }

  @Test
  public void testGroupByAttrOrder() throws Exception {
    SyntaxTree tree = parser.parse("select sum(c) as c, a, a as d, sum(b) as b from large group by a;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("c", 6.0, "a", "a", "d", "a", "b", 4L), reader.readColumns());
    assertEquals(ImmutableMap.of("c", 14.0, "a", "b", "d", "b", "b", 12L), reader.readColumns());
  }

  @Test
  public void testGroupByAndWhere() throws Exception {
    SyntaxTree tree = parser.parse("select a, sum(b) as b from large where c % 4 = 0 group by a;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "a", "b", 3L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L), reader.readColumns());
  }

  @Test
  public void testHaving() throws Exception {
    SyntaxTree tree = parser.parse("select a from large group by a having sum(b) > 10;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "b"), reader.readColumns());
  }

  @Test
  public void testOrderBy() throws Exception {
    SyntaxTree tree = parser.parse("select b from large order by b desc;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("b", 7L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 3L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 1L), reader.readColumns());
  }

  @Test
  public void testOrderByMultipleKeys() throws Exception {
    SyntaxTree tree = parser.parse("select a, b from large order by a desc, b;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "b", "b", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 3L), reader.readColumns());
  }
  
  @Test(expected = ModelException.class)
  public void testAggrInAggr() throws Exception {
    SyntaxTree tree = parser.parse("select sum(sum(b)) from large;");
    compiler.query(tree);
  }

  @Test(expected = ModelException.class)
  public void testNonKeyOutsideOfAggr() throws Exception {
    SyntaxTree tree = parser.parse("select b, sum(c) from large group by a;");
    compiler.query(tree);
  }
}
