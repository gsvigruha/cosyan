package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.DummyTestBase;
import com.cosyan.db.io.IOTestUtil.DummyMaterializedTableMeta;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableMap;

public class TableReaderTest extends DummyTestBase {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParserException, ParseException {
    DummyTestBase.setUp();

    register(new DummyMaterializedTableMeta("table",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.StringType, true, false, false, false),
            "b", new BasicColumn(1, "b", DataTypes.LongType, true, false, false, false),
            "c", new BasicColumn(2, "c", DataTypes.DoubleType, true, false, false, false)),
        new Object[][] {
            new Object[] { "abc", 1L, 1.0 },
            new Object[] { "xyz", 5L, 6.7 } }));

    register(new DummyMaterializedTableMeta("large",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.StringType, true, false, false, false),
            "b", new BasicColumn(1, "b", DataTypes.LongType, true, false, false, false),
            "c", new BasicColumn(2, "c", DataTypes.DoubleType, true, false, false, false)),
        new Object[][] {
            new Object[] { "a", 1L, 2.0 },
            new Object[] { "a", 3L, 4.0 },
            new Object[] { "b", 5L, 6.0 },
            new Object[] { "b", 7L, 8.0 } }));

    register(new DummyMaterializedTableMeta("left",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.StringType, true, false, false, false),
            "b", new BasicColumn(1, "b", DataTypes.LongType, true, false, false, false)),
        new Object[][] {
            new Object[] { "a", 1L },
            new Object[] { "b", 1L },
            new Object[] { "c", 5L } }));

    register(new DummyMaterializedTableMeta("right",
        ImmutableMap.of(
            "x", new BasicColumn(0, "x", DataTypes.StringType, true, false, false, false),
            "y", new BasicColumn(1, "y", DataTypes.LongType, true, false, false, false)),
        new Object[][] {
            new Object[] { "a", 2L },
            new Object[] { "c", 6L } }));

    register(new DummyMaterializedTableMeta("dupl",
        ImmutableMap.of(
            "x", new BasicColumn(0, "x", DataTypes.StringType, true, false, false, false),
            "y", new BasicColumn(1, "y", DataTypes.LongType, true, false, false, false)),
        new Object[][] {
            new Object[] { "a", 1L },
            new Object[] { "a", 5L } }));

    register(new DummyMaterializedTableMeta("null",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.StringType, true, false, false, false),
            "b", new BasicColumn(1, "b", DataTypes.LongType, true, false, false, false),
            "c", new BasicColumn(2, "c", DataTypes.DoubleType, true, false, false, false)),
        new Object[][] {
            new Object[] { DataTypes.NULL, 1L, 2.0 },
            new Object[] { "b", DataTypes.NULL, 4.0 },
            new Object[] { "c", 5L, DataTypes.NULL } }));

    register(new DummyMaterializedTableMeta("dates",
        ImmutableMap.of(
            "a", new BasicColumn(0, "a", DataTypes.DateType, true, false, false, false)),
        new Object[][] {
            new Object[] { sdf.parse("20170101") },
            new Object[] { sdf.parse("20170201") } }));
    finalizeResources();
  }

  @Test
  public void testReadFirstLine() throws Exception {
    ExposedTableReader reader = query("select * from table;");
    assertEquals(ImmutableMap.of("a", "abc", "b", 1L, "c", 1.0), reader.readColumns());
  }

  @Test
  public void testTableAlias() throws Exception {
    ExposedTableReader reader = query("select t.a from table as t;");
    assertEquals(ImmutableMap.of("a", "abc"), reader.readColumns());
  }

  @Test
  public void testReadArithmeticExpressions1() throws Exception {
    ExposedTableReader reader = query("select b + 2, c * 3.0, c / b, c - 1, 3 % 2 from table;");
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", 3.0, "_c2", 1.0, "_c3", 0.0, "_c4", 1L), reader.readColumns());
  }

  @Test
  public void testReadArithmeticExpressions2() throws Exception {
    ExposedTableReader reader = query("select a + 'xyz' from table;");
    assertEquals(ImmutableMap.of("_c0", "abcxyz"), reader.readColumns());
  }

  @Test
  public void testReadLogicExpressions1() throws Exception {
    ExposedTableReader reader = query("select b = 1, b < 0.0, c > 0, c <= 1, c >= 2.0 from table;");
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", false),
        reader.readColumns());
  }

  @Test
  public void testReadLogicExpressions2() throws Exception {
    ExposedTableReader reader = query("select a = 'abc', a > 'b', a < 'x', a >= 'ab', a <= 'x' from table;");
    assertEquals(ImmutableMap.of("_c0", true, "_c1", false, "_c2", true, "_c3", true, "_c4", true),
        reader.readColumns());
  }

  @Test
  public void testReadStringFunction() throws Exception {
    ExposedTableReader reader = query("select length(a), upper(a), substr(a, 1, 1) from table;");
    assertEquals(ImmutableMap.of("_c0", 3L, "_c1", "ABC", "_c2", "b"), reader.readColumns());
  }

  @Test
  public void testFuncallOfFuncall() throws Exception {
    ExposedTableReader reader = query("select a.upper().length() as l from table;");
    assertEquals(ImmutableMap.of("l", 3L), reader.readColumns());
  }

  @Test
  public void testWhere() throws Exception {
    ExposedTableReader reader = query("select * from table where b > 1;");
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerSelect() throws Exception {
    ExposedTableReader reader = query("select * from (select * from table where b > 1);");
    assertEquals(ImmutableMap.of("a", "xyz", "b", 5L, "c", 6.7), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testColumnAliasing() throws Exception {
    ExposedTableReader reader = query("select b + 2 as x, c * 3.0 as y from table;");
    assertEquals(ImmutableMap.of("x", 3L, "y", 3.0), reader.readColumns());
  }

  @Test
  public void testTableAliasing() throws Exception {
    ExposedTableReader reader = query("select t.b from table as t;");
    assertEquals(ImmutableMap.of("b", 1L), reader.readColumns());
  }

  @Test
  public void testGlobalAggregate() throws Exception {
    ExposedTableReader reader = query("select sum(b) as b, sum(c) as c from large;");
    assertEquals(ImmutableMap.of("b", 16L, "c", 20.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testAggregatorsSum() throws Exception {
    ExposedTableReader reader = query("select sum(1) as s1, sum(2.0) as s2, sum(b) as sb, sum(c) as sc from large;");
    assertEquals(ImmutableMap.of("s1", 4L, "s2", 8.0, "sb", 16L, "sc", 20.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testAggregatorsCount() throws Exception {
    ExposedTableReader reader = query(
        "select count(1) as c1, count(a) as ca, count(b) as cb, count(c) as cc from large;");
    assertEquals(ImmutableMap.of("c1", 4L, "ca", 4L, "cb", 4L, "cc", 4L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testAggregatorsMax() throws Exception {
    ExposedTableReader reader = query("select max(a) as a, max(b) as b, max(c) as c from large;");
    assertEquals(ImmutableMap.of("a", "b", "b", 7L, "c", 8.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testAggregatorsMin() throws Exception {
    ExposedTableReader reader = query("select min(a) as a, min(b) as b, min(c) as c from large;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testAggregatorsFuncallOnColumn() throws Exception {
    ExposedTableReader reader = query("select a.max() as a, b.count() as b, c.sum() as c from large;");
    assertEquals(ImmutableMap.of("a", "b", "b", 4L, "c", 20.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testGroupBy() throws Exception {
    ExposedTableReader reader = query("select a, sum(b) as b, sum(c) as c from large group by a;");
    assertEquals(ImmutableMap.of("a", "a", "b", 4L, "c", 6.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 12L, "c", 14.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testExpressionInGroupBy() throws Exception {
    ExposedTableReader reader = query("select a, sum(b + 1) as b, sum(c * 2.0) as c from large group by a;");
    assertEquals(ImmutableMap.of("a", "a", "b", 6L, "c", 12.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 14L, "c", 28.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testExpressionFromGroupBy() throws Exception {
    ExposedTableReader reader = query("select a, sum(b) + sum(c) as b from large group by a;");
    assertEquals(ImmutableMap.of("a", "a", "b", 10.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 26.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testGroupByMultipleKey() throws Exception {
    ExposedTableReader reader = query("select a, b, sum(c) as c from large group by a, b;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 5L, "c", 6.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L, "c", 8.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testGroupByAttrOrder() throws Exception {
    ExposedTableReader reader = query("select sum(c) as c, a, a as d, sum(b) as b from large group by a;");
    assertEquals(ImmutableMap.of("c", 6.0, "a", "a", "d", "a", "b", 4L), reader.readColumns());
    assertEquals(ImmutableMap.of("c", 14.0, "a", "b", "d", "b", "b", 12L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testGroupByAndWhere() throws Exception {
    ExposedTableReader reader = query("select a, sum(b) as b from large where c % 4 = 0 group by a;");
    assertEquals(ImmutableMap.of("a", "a", "b", 3L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testHaving() throws Exception {
    ExposedTableReader reader = query("select a from large group by a having sum(b) > 10;");
    assertEquals(ImmutableMap.of("a", "b"), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testOrderBy() throws Exception {
    ExposedTableReader reader = query("select b from large order by b desc;");
    assertEquals(ImmutableMap.of("b", 7L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 3L), reader.readColumns());
    assertEquals(ImmutableMap.of("b", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testOrderByMultipleKeys() throws Exception {
    ExposedTableReader reader = query("select a, b from large order by a desc, b;");
    assertEquals(ImmutableMap.of("a", "b", "b", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 3L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoin1() throws Exception {
    ExposedTableReader reader = query("select * from left inner join right on a = x;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "x", "c", "y", 6L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoin2() throws Exception {
    ExposedTableReader reader = query("select * from right inner join left on x = a;");
    assertEquals(ImmutableMap.of("x", "a", "y", 2L, "a", "a", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", "c", "y", 6L, "a", "c", "b", 5L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinDuplication1() throws Exception {
    ExposedTableReader reader = query("select * from left inner join dupl on a = x;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 5L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinDuplication2() throws Exception {
    ExposedTableReader reader = query("select * from dupl inner join left on x = a;");
    assertEquals(ImmutableMap.of("x", "a", "y", 1L, "a", "a", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", "a", "y", 5L, "a", "a", "b", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinTableAlias() throws Exception {
    ExposedTableReader reader = query("select l.a, l.b, r.x, r.y from left as l inner join right as r on l.a = r.x;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "x", "c", "y", 6L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinSubSelectAlias() throws Exception {
    ExposedTableReader reader = query("select l.a, r.x from left as l inner join "
        + "(select x from right) as r on l.a = r.x;");
    assertEquals(ImmutableMap.of("a", "a", "x", "a"), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "x", "c"), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinAliasSolvesNameCollision() throws Exception {
    ExposedTableReader reader = query("select l.a as l, r.a as r from left as l inner join "
        + "(select x as a from right) as r on l.a = r.a;");
    assertEquals(ImmutableMap.of("l", "a", "r", "a"), reader.readColumns());
    assertEquals(ImmutableMap.of("l", "c", "r", "c"), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInnerJoinOnExpr() throws Exception {
    ExposedTableReader reader = query("select count(1) as cnt from left inner join right on length(a) = length(x);");
    assertEquals(ImmutableMap.of("cnt", 6L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testLeftJoin() throws Exception {
    ExposedTableReader reader = query("select * from left left join right on a = x;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 1L, "x", DataTypes.NULL, "y", DataTypes.NULL), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "x", "c", "y", 6L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testLeftJoinDuplication() throws Exception {
    ExposedTableReader reader = query("select * from left left join dupl on a = x;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "x", "a", "y", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 1L, "x", DataTypes.NULL, "y", DataTypes.NULL), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "x", DataTypes.NULL, "y", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testRightJoin() throws Exception {
    ExposedTableReader reader = query("select * from right right join left on x = a;");
    assertEquals(ImmutableMap.of("x", "a", "y", 2L, "a", "a", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", DataTypes.NULL, "y", DataTypes.NULL, "a", "b", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", "c", "y", 6L, "a", "c", "b", 5L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testRightJoinDuplication() throws Exception {
    ExposedTableReader reader = query(
        "select d.x, d.y, r.x as x2, r.y as y2 from dupl as d right join right as r on d.x = r.x;");
    assertEquals(ImmutableMap.of("x", "a", "y", 1L, "x2", "a", "y2", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", "a", "y", 5L, "x2", "a", "y2", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("x", DataTypes.NULL, "y", DataTypes.NULL, "x2", "c", "y2", 6L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testReadLinesWithNull() throws Exception {
    ExposedTableReader reader = query("select * from null;");
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", DataTypes.NULL, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "c", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testNullInBinaryExpression() throws Exception {
    ExposedTableReader reader = query("select a + 'x' as a, b * 2 as b, c - 1 as c from null;");
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 2L, "c", 1.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "bx", "b", DataTypes.NULL, "c", 3.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "cx", "b", 10L, "c", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testNullInFuncCall() throws Exception {
    ExposedTableReader reader = query("select length(a) as a from null;");
    assertEquals(ImmutableMap.of("a", DataTypes.NULL), reader.readColumns());
    assertEquals(ImmutableMap.of("a", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testNullInAggregation() throws Exception {
    ExposedTableReader reader = query("select sum(b) as b, count(c) as c from null;");
    assertEquals(ImmutableMap.of("b", 6L, "c", 2L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testNullAsAggregationKey() throws Exception {
    ExposedTableReader reader = query("select a, sum(b) as b from null group by a;");
    assertEquals(ImmutableMap.of("a", "b", "b", DataTypes.NULL), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testOrderByNull() throws Exception {
    ExposedTableReader reader = query("select * from null order by b;");
    assertEquals(ImmutableMap.of("a", "b", "b", DataTypes.NULL, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "c", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test(expected = ModelException.class)
  public void testNullEquals() throws Exception {
    ExposedTableReader reader = query("select * from null where b = null;");
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testWhereIsNull() throws Exception {
    ExposedTableReader reader = query("select * from null where b is null;");
    assertEquals(ImmutableMap.of("a", "b", "b", DataTypes.NULL, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testWhereIsNotNull() throws Exception {
    ExposedTableReader reader = query("select * from null where b is not null;");
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "c", "b", 5L, "c", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDistinctSame() throws Exception {
    ExposedTableReader reader = query("select distinct * from large;");
    assertEquals(ImmutableMap.of("a", "a", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "a", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 5L, "c", 6.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 7L, "c", 8.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDistinctDifferent() throws Exception {
    ExposedTableReader reader = query("select distinct a from large;");
    assertEquals(ImmutableMap.of("a", "a"), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b"), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testCountDistinctGlobal() throws Exception {
    ExposedTableReader reader = query("select count(distinct a) as a, count(distinct b) as b from large;");
    assertEquals(ImmutableMap.of("a", 2L, "b", 4L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testCountDistinctGroupBy() throws Exception {
    ExposedTableReader reader = query("select a, count(distinct b) as b from large group by a;");
    assertEquals(ImmutableMap.of("a", "a", "b", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "b", "b", 2L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testListAggr() throws Exception {
    ExposedTableReader reader = query("select a, list(b) as b from large group by a;");
    assertArrayEquals(new Long[] { 1L, 3L }, (Long[]) reader.readColumns().get("b"));
    assertArrayEquals(new Long[] { 5L, 7L }, (Long[]) reader.readColumns().get("b"));
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testCase() throws Exception {
    ExposedTableReader reader = query("select case when a = 'abc' then b else c end as a from table;");
    assertEquals(ImmutableMap.of("a", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", 6.7), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDates() throws Exception {
    ExposedTableReader reader = query("select a from dates;");
    assertEquals(ImmutableMap.of("a", sdf.parse("20170101")), reader.readColumns());
    assertEquals(ImmutableMap.of("a", sdf.parse("20170201")), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDateFunctions() throws Exception {
    ExposedTableReader reader = query("select "
        + "a > date('2017-01-15') as x, "
        + "a > date('2017-01-15 00:00:00') as y, "
        + "date('20170115') as z from dates;");
    assertEquals(ImmutableMap.of("x", false, "y", false, "z", DataTypes.NULL), reader.readColumns());
    assertEquals(ImmutableMap.of("x", true, "y", true, "z", DataTypes.NULL), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test(expected = ModelException.class)
  public void testAggrInAggr() throws Exception {
    query("select sum(sum(b)) from large;");
  }

  @Test(expected = ModelException.class)
  public void testNonKeyOutsideOfAggr() throws Exception {
    query("select b, sum(c) from large group by a;");
  }

  @Test(expected = ModelException.class)
  public void testGroupByInconsistentAggr() throws Exception {
    query("select sum(b) + b from large group by a;");
  }

  @Test(expected = ModelException.class)
  public void testInconsistentAggr() throws Exception {
    query("select sum(b) + b from large;");
  }
}
