package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.NotAggrTableException;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;

public class TableReaderTest extends UnitTestBase {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
  private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd hhmmss");

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParserException, ConfigException {
    UnitTestBase.setUp();

    execute("create table table (a varchar, b integer, c float);");
    execute("insert into table values ('abc', 1, 1.0), ('xyz', 5, 6.7);");

    execute("create table large (a varchar, b integer, c float);");
    execute("insert into large values ('a', 1, 2.0), ('a', 3, 4.0), ('b', 5, 6.0), ('b', 7, 8.0);");

    execute("create table left (a varchar, b integer);");
    execute("insert into left values ('a', 1), ('b', 1), ('c', 5);");

    execute("create table right (x varchar, y integer);");
    execute("insert into right values ('a', 2), ('c', 6);");

    execute("create table dupl (x varchar, y integer);");
    execute("insert into dupl values ('a', 1), ('a', 5);");

    execute("create table null (a varchar, b integer, c float);");
    execute("insert into null (b, c) values (1, 2.0);");
    execute("insert into null (a, c) values ('b', 4.0);");
    execute("insert into null (a, b) values ('c', 5);");

    execute("create table dates (a timestamp);");
    execute("insert into dates values (dt '2017-01-01'), (dt '2017-02-01');");

    execute("create table stats (b integer, c float);");
    execute("insert into stats values (1, 9.0), (8, 13.0), (12, 40.0), (11, 14.0), (6, 21.0);");
  }

  @Test
  public void testReadFirstLine() throws Exception {
    QueryResult result = query("select * from table;");
    assertEquals(ImmutableList.of("abc", 1L, 1.0), result.getValues().get(0));
  }

  @Test
  public void testTableAlias() throws Exception {
    QueryResult result = query("select t.a from table as t;");
    assertEquals(ImmutableList.of("abc"), result.getValues().get(0));
  }

  @Test
  public void testReadArithmeticExpressions1() throws Exception {
    QueryResult result = query("select b + 2, c * 3.0, c / b, c - 1, 3 % 2 from table;");
    assertEquals(ImmutableList.of(3L, 3.0, 1.0, 0.0, 1L), result.getValues().get(0));
  }

  @Test
  public void testReadArithmeticExpressions2() throws Exception {
    QueryResult result = query("select a + 'xyz' from table;");
    assertEquals(ImmutableList.of("abcxyz"), result.getValues().get(0));
  }

  @Test
  public void testReadLogicExpressions1() throws Exception {
    QueryResult result = query("select b = 1, b < 0.0, c > 0, c <= 1, c >= 2.0 from table;");
    assertEquals(ImmutableList.of(true, false, true, true, false), result.getValues().get(0));
  }

  @Test
  public void testReadLogicExpressions2() throws Exception {
    QueryResult result = query("select a = 'abc', a > 'b', a < 'x', a >= 'ab', a <= 'x' from table;");
    assertEquals(ImmutableList.of(true, false, true, true, true), result.getValues().get(0));
  }

  @Test
  public void testReadStringFunction() throws Exception {
    QueryResult result = query("select length(a), upper(a), substr(a, 1, 1) from table;");
    assertEquals(ImmutableList.of(3L, "ABC", "b"), result.getValues().get(0));
  }

  @Test
  public void testFuncallOfFuncall() throws Exception {
    QueryResult result = query("select a.upper().length() as l from table;");
    assertEquals(ImmutableList.of(3L), result.getValues().get(0));
  }

  @Test
  public void testWhere() throws Exception {
    QueryResult result = query("select * from table where b > 1;");
    assertEquals(ImmutableList.of("xyz", 5L, 6.7), result.getValues().get(0));
  }

  @Test
  public void testInnerSelect() throws Exception {
    QueryResult result = query("select * from (select * from table where b > 1);");
    assertEquals(ImmutableList.of("xyz", 5L, 6.7), result.getValues().get(0));
  }

  @Test
  public void testColumnAliasing() throws Exception {
    QueryResult result = query("select b + 2 as x, c * 3.0 as y from table;");
    assertEquals(ImmutableList.of(3L, 3.0), result.getValues().get(0));
  }

  @Test
  public void testTableAliasing() throws Exception {
    QueryResult result = query("select t.b from table as t;");
    assertEquals(ImmutableList.of(1L), result.getValues().get(0));
  }

  @Test
  public void testGlobalAggregate() throws Exception {
    QueryResult result = query("select sum(b) as b, sum(c) as c from large;");
    assertEquals(ImmutableList.of(16L, 20.0), result.getValues().get(0));
  }

  @Test
  public void testAggregatorsSum() throws Exception {
    QueryResult result = query("select sum(1) as s1, sum(2.0) as s2, sum(b) as sb, sum(c) as sc from large;");
    assertEquals(ImmutableList.of(4L, 8.0, 16L, 20.0), result.getValues().get(0));
  }

  @Test
  public void testAggregatorsCount() throws Exception {
    QueryResult result = query(
        "select count(1) as c1, count(a) as ca, count(b) as cb, count(c) as cc from large;");
    assertEquals(ImmutableList.of(4L, 4L, 4L, 4L), result.getValues().get(0));
  }

  @Test
  public void testAggregatorsMax() throws Exception {
    QueryResult result = query("select max(a) as a, max(b) as b, max(c) as c from large;");
    assertEquals(ImmutableList.of("b", 7L, 8.0), result.getValues().get(0));
  }

  @Test
  public void testAggregatorsMin() throws Exception {
    QueryResult result = query("select min(a) as a, min(b) as b, min(c) as c from large;");
    assertEquals(ImmutableList.of("a", 1L, 2.0), result.getValues().get(0));
  }

  @Test
  public void testAggregatorsFuncallOnColumn() throws Exception {
    QueryResult result = query("select a.max() as a, b.count() as b, c.sum() as c from large;");
    assertEquals(ImmutableList.of("b", 4L, 20.0), result.getValues().get(0));
  }

  @Test
  public void testGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b) as b, sum(c) as c from large group by a;");
    assertEquals(ImmutableList.of("a", 4L, 6.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 12L, 14.0), result.getValues().get(1));
  }

  @Test
  public void testExpressionInGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b + 1) as b, sum(c * 2.0) as c from large group by a;");
    assertEquals(ImmutableList.of("a", 6L, 12.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 14L, 28.0), result.getValues().get(1));
  }

  @Test
  public void testExpressionFromGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b) + sum(c) as b from large group by a;");
    assertEquals(ImmutableList.of("a", 10.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 26.0), result.getValues().get(1));
  }

  @Test
  public void testGroupByMultipleKey() throws Exception {
    QueryResult result = query("select a, b, sum(c) as c from large group by a, b;");
    assertEquals(ImmutableList.of("a", 1L, 2.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 3L, 4.0), result.getValues().get(1));
    assertEquals(ImmutableList.of("b", 5L, 6.0), result.getValues().get(2));
    assertEquals(ImmutableList.of("b", 7L, 8.0), result.getValues().get(3));
  }

  @Test
  public void testGroupByAttrOrder() throws Exception {
    QueryResult result = query("select sum(c) as c, a, a as d, sum(b) as b from large group by a;");
    assertEquals(ImmutableList.of(6.0, "a", "a", 4L), result.getValues().get(0));
    assertEquals(ImmutableList.of(14.0, "b", "b", 12L), result.getValues().get(1));
  }

  @Test
  public void testGroupByAndWhere() throws Exception {
    QueryResult result = query("select a, sum(b) as b from large where c % 4 = 0 group by a;");
    assertEquals(ImmutableList.of("a", 3L), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 7L), result.getValues().get(1));
  }

  @Test
  public void testHaving() throws Exception {
    QueryResult result = query("select a from large group by a having sum(b) > 10;");
    assertEquals(ImmutableList.of("b"), result.getValues().get(0));
  }

  @Test
  public void testOrderBy() throws Exception {
    QueryResult result = query("select b from large order by b desc;");
    assertEquals(ImmutableList.of(7L), result.getValues().get(0));
    assertEquals(ImmutableList.of(5L), result.getValues().get(1));
    assertEquals(ImmutableList.of(3L), result.getValues().get(2));
    assertEquals(ImmutableList.of(1L), result.getValues().get(3));
  }

  @Test
  public void testOrderByMultipleKeys() throws Exception {
    QueryResult result = query("select a, b from large order by a desc, b;");
    assertEquals(ImmutableList.of("b", 5L), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 7L), result.getValues().get(1));
    assertEquals(ImmutableList.of("a", 1L), result.getValues().get(2));
    assertEquals(ImmutableList.of("a", 3L), result.getValues().get(3));
  }

  @Test
  public void testInnerJoin1() throws Exception {
    QueryResult result = query("select * from left inner join right on a = x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 2L), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", 5L, "c", 6L), result.getValues().get(1));
  }

  @Test
  public void testInnerJoin2() throws Exception {
    QueryResult result = query("select * from right inner join left on x = a;");
    assertEquals(ImmutableList.of("a", 2L, "a", 1L), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", 6L, "c", 5L), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinDuplication1() throws Exception {
    QueryResult result = query("select * from left inner join dupl on a = x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 1L), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 1L, "a", 5L), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinDuplication2() throws Exception {
    QueryResult result = query("select * from dupl inner join left on x = a;");
    assertEquals(ImmutableList.of("a", 1L, "a", 1L), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 5L, "a", 1L), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinTableAlias() throws Exception {
    QueryResult result = query("select l.a, l.b, r.x, r.y from left as l inner join right as r on l.a = r.x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 2L), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", 5L, "c", 6L), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinSubSelectAlias() throws Exception {
    QueryResult result = query("select l.a, r.x from left as l inner join "
        + "(select x from right) as r on l.a = r.x;");
    assertEquals(ImmutableList.of("a", "a"), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", "c"), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinAliasSolvesNameCollision() throws Exception {
    QueryResult result = query("select l.a as l, r.a as r from left as l inner join "
        + "(select x as a from right) as r on l.a = r.a;");
    assertEquals(ImmutableList.of("a", "a"), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", "c"), result.getValues().get(1));
  }

  @Test
  public void testInnerJoinOnExpr() throws Exception {
    QueryResult result = query("select count(1) as cnt from left inner join right on length(a) = length(x);");
    assertEquals(ImmutableList.of(6L), result.getValues().get(0));
  }

  @Test
  public void testLeftJoin() throws Exception {
    QueryResult result = query("select * from left left join right on a = x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 2L), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 1L, DataTypes.NULL, DataTypes.NULL),
        result.getValues().get(1));
    assertEquals(ImmutableList.of("c", 5L, "c", 6L), result.getValues().get(2));
  }

  @Test
  public void testLeftJoinDuplication() throws Exception {
    QueryResult result = query("select * from left left join dupl on a = x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 1L), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 1L, "a", 5L), result.getValues().get(1));
    assertEquals(ImmutableList.of("b", 1L, DataTypes.NULL, DataTypes.NULL),
        result.getValues().get(2));
    assertEquals(ImmutableList.of("c", 5L, DataTypes.NULL, DataTypes.NULL),
        result.getValues().get(3));
  }

  @Test
  public void testRightJoin() throws Exception {
    QueryResult result = query("select * from right right join left on x = a;");
    assertEquals(ImmutableList.of("a", 2L, "a", 1L), result.getValues().get(0));
    assertEquals(ImmutableList.of(DataTypes.NULL, DataTypes.NULL, "b", 1L),
        result.getValues().get(1));
    assertEquals(ImmutableList.of("c", 6L, "c", 5L), result.getValues().get(2));
  }

  @Test
  public void testRightJoinDuplication() throws Exception {
    QueryResult result = query(
        "select d.x, d.y, r.x as x2, r.y as y2 from dupl as d right join right as r on d.x = r.x;");
    assertEquals(ImmutableList.of("a", 1L, "a", 2L), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 5L, "a", 2L), result.getValues().get(1));
    assertEquals(ImmutableList.of(DataTypes.NULL, DataTypes.NULL, "c", 6L), result.getValues().get(2));
  }

  @Test
  public void testReadLinesWithNull() throws Exception {
    QueryResult result = query("select * from null;");
    assertEquals(ImmutableList.of(DataTypes.NULL, 1L, 2.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", DataTypes.NULL, 4.0), result.getValues().get(1));
    assertEquals(ImmutableList.of("c", 5L, DataTypes.NULL), result.getValues().get(2));
  }

  @Test
  public void testNullInBinaryExpression() throws Exception {
    QueryResult result = query("select a + 'x' as a, b * 2 as b, c - 1 as c from null;");
    assertEquals(ImmutableList.of(DataTypes.NULL, 2L, 1.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("bx", DataTypes.NULL, 3.0), result.getValues().get(1));
    assertEquals(ImmutableList.of("cx", 10L, DataTypes.NULL), result.getValues().get(2));
  }

  @Test
  public void testNullInFuncCall() throws Exception {
    QueryResult result = query("select length(a) as a from null;");
    assertEquals(ImmutableList.of(DataTypes.NULL), result.getValues().get(0));
    assertEquals(ImmutableList.of(1L), result.getValues().get(1));
    assertEquals(ImmutableList.of(1L), result.getValues().get(2));
  }

  @Test
  public void testNullInAggregation() throws Exception {
    QueryResult result = query("select sum(b) as b, count(c) as c from null;");
    assertEquals(ImmutableList.of(6L, 2L), result.getValues().get(0));
  }

  @Test
  public void testNullAsAggregationKey() throws Exception {
    QueryResult result = query("select a, sum(b) as b from null group by a;");
    assertEquals(ImmutableList.of("b", DataTypes.NULL), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", 5L), result.getValues().get(1));
    assertEquals(ImmutableList.of(DataTypes.NULL, 1L), result.getValues().get(2));
  }

  @Test
  public void testStdDev() throws Exception {
    QueryResult result = query("select "
        + "round_to(stddev(b), 3) as b1, round_to(stddev(c), 3) as c1,"
        + "round_to(stddev_pop(b), 3) as b2, round_to(stddev_pop(c), 3) as c2 from large;");
    assertEquals(ImmutableList.of(2.582, 2.582, 2.236, 2.236), result.getValues().get(0));
  }

  @Test
  public void testSkewnessAndKurtosis() throws Exception {
    QueryResult result = query("select "
        + "round_to(skewness(b), 3) as b1, round_to(skewness(c), 3) as c1,"
        + "round_to(kurtosis(b), 3) as b2, round_to(kurtosis(c), 3) as c2 from stats;");
    assertEquals(ImmutableList.of(-0.493, 0.967, 1.619, 2.122), result.getValues().get(0));
  }

  @Test
  public void testOrderByNull() throws Exception {
    QueryResult result = query("select * from null order by b;");
    assertEquals(ImmutableList.of("b", DataTypes.NULL, 4.0), result.getValues().get(0));
    assertEquals(ImmutableList.of(DataTypes.NULL, 1L, 2.0), result.getValues().get(1));
    assertEquals(ImmutableList.of("c", 5L, DataTypes.NULL), result.getValues().get(2));
  }

  @Test
  public void testNullEquals() throws Exception {
    ErrorResult e = error("select * from null where b = null;");
    assertError(ModelException.class, "Unsupported binary expression = for types integer and null.", e);
  }

  @Test
  public void testWhereIsNull() throws Exception {
    QueryResult result = query("select * from null where b is null;");
    assertEquals(ImmutableList.of("b", DataTypes.NULL, 4.0), result.getValues().get(0));
  }

  @Test
  public void testWhereIsNotNull() throws Exception {
    QueryResult result = query("select * from null where b is not null;");
    assertEquals(ImmutableList.of(DataTypes.NULL, 1L, 2.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("c", 5L, DataTypes.NULL), result.getValues().get(1));
  }

  @Test
  public void testDistinctSame() throws Exception {
    QueryResult result = query("select distinct * from large;");
    assertEquals(ImmutableList.of("a", 1L, 2.0), result.getValues().get(0));
    assertEquals(ImmutableList.of("a", 3L, 4.0), result.getValues().get(1));
    assertEquals(ImmutableList.of("b", 5L, 6.0), result.getValues().get(2));
    assertEquals(ImmutableList.of("b", 7L, 8.0), result.getValues().get(3));
  }

  @Test
  public void testDistinctDifferent() throws Exception {
    QueryResult result = query("select distinct a from large;");
    assertEquals(ImmutableList.of("a"), result.getValues().get(0));
    assertEquals(ImmutableList.of("b"), result.getValues().get(1));
  }

  @Test
  public void testCountDistinctGlobal() throws Exception {
    QueryResult result = query("select count(distinct a) as a, count(distinct b) as b from large;");
    assertEquals(ImmutableList.of(2L, 4L), result.getValues().get(0));
  }

  @Test
  public void testCountDistinctGroupBy() throws Exception {
    QueryResult result = query("select a, count(distinct b) as b from large group by a;");
    assertEquals(ImmutableList.of("a", 2L), result.getValues().get(0));
    assertEquals(ImmutableList.of("b", 2L), result.getValues().get(1));
  }

  @Test
  public void testListAggr() throws Exception {
    QueryResult result = query("select list(b) as b from large group by a;");
    assertArrayEquals(new Long[] { 1L, 3L }, (Long[]) result.getValues().get(0).get(0));
    assertArrayEquals(new Long[] { 5L, 7L }, (Long[]) result.getValues().get(1).get(0));
  }

  @Test
  public void testCase() throws Exception {
    QueryResult result = query("select case when a = 'abc' then b else c end as a from table;");
    assertEquals(ImmutableList.of(1L), result.getValues().get(0));
    assertEquals(ImmutableList.of(6.7), result.getValues().get(1));
  }

  @Test
  public void testDates() throws Exception {
    QueryResult result = query("select a from dates;");
    assertEquals(ImmutableList.of(sdf.parse("20170101")), result.getValues().get(0));
    assertEquals(ImmutableList.of(sdf.parse("20170201")), result.getValues().get(1));
  }

  @Test
  public void testDateFunctions1() throws Exception {
    QueryResult result = query("select "
        + "a > date('2017-01-15') as x, "
        + "a > date('2017-01-15 00:00:00') as y, "
        + "date('20170115') as z from dates;");
    assertEquals(ImmutableList.of(false, false, DataTypes.NULL), result.getValues().get(0));
    assertEquals(ImmutableList.of(true, true, DataTypes.NULL), result.getValues().get(1));
  }

  @Test
  public void testDateFunctions_Add() throws Exception {
    QueryResult result = query("select "
        + "add_years(a, 1) as a, "
        + "add_months(a, 1) as b, "
        + "add_weeks(a, 1) as c, "
        + "add_days(a, 1) as d, "
        + "add_hours(a, 1) as e, "
        + "add_minutes(a, 1) as f, "
        + "add_seconds(a, 1) as g "
        + "from dates;");
    assertEquals(ImmutableList.builder()
        .add(sdf.parse("20180101"))
        .add(sdf.parse("20170201"))
        .add(sdf.parse("20170108"))
        .add(sdf.parse("20170102"))
        .add(sdf2.parse("20170101 010000"))
        .add(sdf2.parse("20170101 000100"))
        .add(sdf2.parse("20170101 000001")).build(), result.getValues().get(0));
  }

  @Test
  public void testAggrInAggr() throws Exception {
    ErrorResult e = error("select sum(sum(b)) from large;");
    assertError(NotAggrTableException.class, "", e);
  }

  @Test
  public void testNonKeyOutsideOfAggr() throws Exception {
    ErrorResult e = error("select b, sum(c) from large group by a;");
    assertError(ModelException.class, "Column 'b' not found in table.", e);
  }

  @Test
  public void testGroupByInconsistentAggr() throws Exception {
    ErrorResult e = error("select sum(b) + b from large group by a;");
    assertError(ModelException.class, "Column 'b' not found in table.", e);
  }

  @Test
  public void testInconsistentAggr() throws Exception {
    ErrorResult e = error("select sum(b) + b from large;");
    assertError(ModelException.class, "Column 'b' not found in table.", e);
  }

  @Test
  public void testWrongArgNumber() throws Exception {
    ErrorResult e = error("select length(a, 1) from table;");
    assertError(ModelException.class, "Expected 1 columns for function 'length' but got 2.", e);
  }
}
