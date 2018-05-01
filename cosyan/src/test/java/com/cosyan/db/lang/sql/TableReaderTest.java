package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertArrayEquals;

import java.text.SimpleDateFormat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables.NotAggrTableException;

public class TableReaderTest extends UnitTestBase {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
  private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd hhmmss");

  @BeforeClass
  public static void setUp() throws Exception {
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

    execute("create table enums (a enum('x', 'y'));");
    execute("insert into enums values ('x'), ('y');");
  }

  @Test
  public void testReadFirstLine() throws Exception {
    QueryResult result = query("select * from table;");
    assertArrayEquals(new Object[] { "abc", 1L, 1.0 }, result.getValues().get(0));
  }

  @Test
  public void testTableAlias() throws Exception {
    QueryResult result = query("select t.a from table as t;");
    assertArrayEquals(new Object[] { "abc" }, result.getValues().get(0));
  }

  @Test
  public void testReadArithmeticExpressions1() throws Exception {
    QueryResult result = query("select b + 2, c * 3.0, c / b, c - 1, 3 % 2 from table;");
    assertArrayEquals(new Object[] { 3L, 3.0, 1.0, 0.0, 1L }, result.getValues().get(0));
  }

  @Test
  public void testReadArithmeticExpressions2() throws Exception {
    QueryResult result = query("select a + 'xyz' from table;");
    assertArrayEquals(new Object[] { "abcxyz" }, result.getValues().get(0));
  }

  @Test
  public void testReadLogicExpressions1() throws Exception {
    QueryResult result = query("select b = 1, b < 0.0, c > 0, c <= 1, c >= 2.0 from table;");
    assertArrayEquals(new Object[] { true, false, true, true, false }, result.getValues().get(0));
  }

  @Test
  public void testReadLogicExpressions2() throws Exception {
    QueryResult result = query("select a = 'abc', a > 'b', a < 'x', a >= 'ab', a <= 'x' from table;");
    assertArrayEquals(new Object[] { true, false, true, true, true }, result.getValues().get(0));
  }

  @Test
  public void testReadStringFunction() throws Exception {
    QueryResult result = query("select length(a), upper(a), substr(a, 1, 1) from table;");
    assertArrayEquals(new Object[] { 3L, "ABC", "b" }, result.getValues().get(0));
  }

  @Test
  public void testFuncallOfFuncall() throws Exception {
    QueryResult result = query("select a.upper().length() as l from table;");
    assertArrayEquals(new Object[] { 3L }, result.getValues().get(0));
  }

  @Test
  public void testWhere() throws Exception {
    QueryResult result = query("select * from table where b > 1;");
    assertArrayEquals(new Object[] { "xyz", 5L, 6.7 }, result.getValues().get(0));
  }

  @Test
  public void testInnerSelect() throws Exception {
    QueryResult result = query("select * from (select * from table where b > 1);");
    assertArrayEquals(new Object[] { "xyz", 5L, 6.7 }, result.getValues().get(0));
  }

  @Test
  public void testColumnAliasing() throws Exception {
    QueryResult result = query("select b + 2 as x, c * 3.0 as y from table;");
    assertArrayEquals(new Object[] { 3L, 3.0 }, result.getValues().get(0));
  }

  @Test
  public void testTableAliasing() throws Exception {
    QueryResult result = query("select t.b from table as t;");
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(0));
  }

  @Test
  public void testGlobalAggregate() throws Exception {
    QueryResult result = query("select sum(b) as b, sum(c) as c from large;");
    assertArrayEquals(new Object[] { 16L, 20.0 }, result.getValues().get(0));
  }

  @Test
  public void testAggregatorsSum() throws Exception {
    QueryResult result = query("select sum(1) as s1, sum(2.0) as s2, sum(b) as sb, sum(c) as sc from large;");
    assertArrayEquals(new Object[] { 4L, 8.0, 16L, 20.0 }, result.getValues().get(0));
  }

  @Test
  public void testAggregatorsCount() throws Exception {
    QueryResult result = query(
        "select count(1) as c1, count(a) as ca, count(b) as cb, count(c) as cc from large;");
    assertArrayEquals(new Object[] { 4L, 4L, 4L, 4L }, result.getValues().get(0));
  }

  @Test
  public void testAggregatorsMax() throws Exception {
    QueryResult result = query("select max(a) as a, max(b) as b, max(c) as c from large;");
    assertArrayEquals(new Object[] { "b", 7L, 8.0 }, result.getValues().get(0));
  }

  @Test
  public void testAggregatorsMin() throws Exception {
    QueryResult result = query("select min(a) as a, min(b) as b, min(c) as c from large;");
    assertArrayEquals(new Object[] { "a", 1L, 2.0 }, result.getValues().get(0));
  }

  @Test
  public void testAggregatorsFuncallOnColumn() throws Exception {
    QueryResult result = query("select a.max() as a, b.count() as b, c.sum() as c from large;");
    assertArrayEquals(new Object[] { "b", 4L, 20.0 }, result.getValues().get(0));
  }

  @Test
  public void testGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b) as b, sum(c) as c from large group by a;");
    assertArrayEquals(new Object[] { "a", 4L, 6.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 12L, 14.0 }, result.getValues().get(1));
  }

  @Test
  public void testExpressionInGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b + 1) as b, sum(c * 2.0) as c from large group by a;");
    assertArrayEquals(new Object[] { "a", 6L, 12.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 14L, 28.0 }, result.getValues().get(1));
  }

  @Test
  public void testExpressionFromGroupBy() throws Exception {
    QueryResult result = query("select a, sum(b) + sum(c) as b from large group by a;");
    assertArrayEquals(new Object[] { "a", 10.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 26.0 }, result.getValues().get(1));
  }

  @Test
  public void testGroupByMultipleKey() throws Exception {
    QueryResult result = query("select a, b, sum(c) as c from large group by a, b;");
    assertArrayEquals(new Object[] { "a", 1L, 2.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 3L, 4.0 }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "b", 5L, 6.0 }, result.getValues().get(2));
    assertArrayEquals(new Object[] { "b", 7L, 8.0 }, result.getValues().get(3));
  }

  @Test
  public void testGroupByAttrOrder() throws Exception {
    QueryResult result = query("select sum(c) as c, a, a as d, sum(b) as b from large group by a;");
    assertArrayEquals(new Object[] { 6.0, "a", "a", 4L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { 14.0, "b", "b", 12L }, result.getValues().get(1));
  }

  @Test
  public void testGroupByAndWhere() throws Exception {
    QueryResult result = query("select a, sum(b) as b from large where c % 4 = 0 group by a;");
    assertArrayEquals(new Object[] { "a", 3L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 7L }, result.getValues().get(1));
  }

  @Test
  public void testHaving() throws Exception {
    QueryResult result = query("select a from large group by a having sum(b) > 10;");
    assertArrayEquals(new Object[] { "b" }, result.getValues().get(0));
  }

  @Test
  public void testOrderBy() throws Exception {
    QueryResult result = query("select b from large order by b desc;");
    assertArrayEquals(new Object[] { 7L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { 5L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { 3L }, result.getValues().get(2));
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(3));
  }

  @Test
  public void testOrderByMultipleKeys() throws Exception {
    QueryResult result = query("select a, b from large order by a desc, b;");
    assertArrayEquals(new Object[] { "b", 5L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 7L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "a", 1L }, result.getValues().get(2));
    assertArrayEquals(new Object[] { "a", 3L }, result.getValues().get(3));
  }

  @Test
  public void testInnerJoin1() throws Exception {
    QueryResult result = query("select * from left inner join right on a = x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 2L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", 5L, "c", 6L }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoin2() throws Exception {
    QueryResult result = query("select * from right inner join left on x = a;");
    assertArrayEquals(new Object[] { "a", 2L, "a", 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", 6L, "c", 5L }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinDuplication1() throws Exception {
    QueryResult result = query("select * from left inner join dupl on a = x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 1L, "a", 5L }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinDuplication2() throws Exception {
    QueryResult result = query("select * from dupl inner join left on x = a;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 5L, "a", 1L }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinTableAlias() throws Exception {
    QueryResult result = query("select l.a, l.b, r.x, r.y from left as l inner join right as r on l.a = r.x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 2L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", 5L, "c", 6L }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinSubSelectAlias() throws Exception {
    QueryResult result = query("select l.a, r.x from left as l inner join "
        + "(select x from right) as r on l.a = r.x;");
    assertArrayEquals(new Object[] { "a", "a" }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", "c" }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinAliasSolvesNameCollision() throws Exception {
    QueryResult result = query("select l.a as l, r.a as r from left as l inner join "
        + "(select x as a from right) as r on l.a = r.a;");
    assertArrayEquals(new Object[] { "a", "a" }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", "c" }, result.getValues().get(1));
  }

  @Test
  public void testInnerJoinOnExpr() throws Exception {
    QueryResult result = query("select count(1) as cnt from left inner join right on length(a) = length(x);");
    assertArrayEquals(new Object[] { 6L }, result.getValues().get(0));
  }

  @Test
  public void testLeftJoin() throws Exception {
    QueryResult result = query("select * from left left join right on a = x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 2L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 1L, null, null },
        result.getValues().get(1));
    assertArrayEquals(new Object[] { "c", 5L, "c", 6L }, result.getValues().get(2));
  }

  @Test
  public void testLeftJoinDuplication() throws Exception {
    QueryResult result = query("select * from left left join dupl on a = x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 1L, "a", 5L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "b", 1L, null, null },
        result.getValues().get(2));
    assertArrayEquals(new Object[] { "c", 5L, null, null },
        result.getValues().get(3));
  }

  @Test
  public void testRightJoin() throws Exception {
    QueryResult result = query("select * from right right join left on x = a;");
    assertArrayEquals(new Object[] { "a", 2L, "a", 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { null, null, "b", 1L },
        result.getValues().get(1));
    assertArrayEquals(new Object[] { "c", 6L, "c", 5L }, result.getValues().get(2));
  }

  @Test
  public void testRightJoinDuplication() throws Exception {
    QueryResult result = query(
        "select d.x, d.y, r.x as x2, r.y as y2 from dupl as d right join right as r on d.x = r.x;");
    assertArrayEquals(new Object[] { "a", 1L, "a", 2L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 5L, "a", 2L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { null, null, "c", 6L }, result.getValues().get(2));
  }

  @Test
  public void testReadLinesWithNull() throws Exception {
    QueryResult result = query("select * from null;");
    assertArrayEquals(new Object[] { null, 1L, 2.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", null, 4.0 }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "c", 5L, null }, result.getValues().get(2));
  }

  @Test
  public void testNullInBinaryExpression() throws Exception {
    QueryResult result = query("select a + 'x' as a, b * 2 as b, c - 1 as c from null;");
    assertArrayEquals(new Object[] { null, 2L, 1.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "bx", null, 3.0 }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "cx", 10L, null }, result.getValues().get(2));
  }

  @Test
  public void testNullInFuncCall() throws Exception {
    QueryResult result = query("select length(a) as a from null;");
    assertArrayEquals(new Object[] { null }, result.getValues().get(0));
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(2));
  }

  @Test
  public void testNullInAggregation() throws Exception {
    QueryResult result = query("select sum(b) as b, count(c) as c from null;");
    assertArrayEquals(new Object[] { 6L, 2L }, result.getValues().get(0));
  }

  @Test
  public void testNullAsAggregationKey() throws Exception {
    QueryResult result = query("select a, sum(b) as b from null group by a;");
    assertArrayEquals(new Object[] { "b", null }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", 5L }, result.getValues().get(1));
    assertArrayEquals(new Object[] { null, 1L }, result.getValues().get(2));
  }

  @Test
  public void testStdDev() throws Exception {
    QueryResult result = query("select "
        + "round_to(stddev(b), 3) as b1, round_to(stddev(c), 3) as c1,"
        + "round_to(stddev_pop(b), 3) as b2, round_to(stddev_pop(c), 3) as c2 from large;");
    assertArrayEquals(new Object[] { 2.582, 2.582, 2.236, 2.236 }, result.getValues().get(0));
  }

  @Test
  public void testSkewnessAndKurtosis() throws Exception {
    QueryResult result = query("select "
        + "round_to(skewness(b), 3) as b1, round_to(skewness(c), 3) as c1,"
        + "round_to(kurtosis(b), 3) as b2, round_to(kurtosis(c), 3) as c2 from stats;");
    assertArrayEquals(new Object[] { -0.493, 0.967, 1.619, 2.122 }, result.getValues().get(0));
  }

  @Test
  public void testOrderByNull() throws Exception {
    QueryResult result = query("select * from null order by b;");
    assertArrayEquals(new Object[] { "b", null, 4.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { null, 1L, 2.0 }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "c", 5L, null }, result.getValues().get(2));
  }

  @Test
  public void testNullEquals() throws Exception {
    ErrorResult e = error("select * from null where b = null;");
    assertError(ModelException.class, "[26, 27]: Unsupported binary expression '=' for types 'integer' and 'null'.", e);
  }

  @Test
  public void testWhereIsNull() throws Exception {
    QueryResult result = query("select * from null where b is null;");
    assertArrayEquals(new Object[] { "b", null, 4.0 }, result.getValues().get(0));
  }

  @Test
  public void testWhereIsNotNull() throws Exception {
    QueryResult result = query("select * from null where b is not null;");
    assertArrayEquals(new Object[] { null, 1L, 2.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "c", 5L, null }, result.getValues().get(1));
  }

  @Test
  public void testDistinctSame() throws Exception {
    QueryResult result = query("select distinct * from large;");
    assertArrayEquals(new Object[] { "a", 1L, 2.0 }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "a", 3L, 4.0 }, result.getValues().get(1));
    assertArrayEquals(new Object[] { "b", 5L, 6.0 }, result.getValues().get(2));
    assertArrayEquals(new Object[] { "b", 7L, 8.0 }, result.getValues().get(3));
  }

  @Test
  public void testDistinctDifferent() throws Exception {
    QueryResult result = query("select distinct a from large;");
    assertArrayEquals(new Object[] { "a" }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b" }, result.getValues().get(1));
  }

  @Test
  public void testCountDistinctGlobal() throws Exception {
    QueryResult result = query("select count(distinct a) as a, count(distinct b) as b from large;");
    assertArrayEquals(new Object[] { 2L, 4L }, result.getValues().get(0));
  }

  @Test
  public void testCountDistinctGroupBy() throws Exception {
    QueryResult result = query("select a, count(distinct b) as b from large group by a;");
    assertArrayEquals(new Object[] { "a", 2L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "b", 2L }, result.getValues().get(1));
  }

  @Test
  public void testListAggr() throws Exception {
    QueryResult result = query("select list(b) as b from large group by a;");
    assertArrayEquals(new Long[] { 1L, 3L }, (Long[]) result.getValues().get(0)[0]);
    assertArrayEquals(new Long[] { 5L, 7L }, (Long[]) result.getValues().get(1)[0]);
  }

  @Test
  public void testSetAggr() throws Exception {
    QueryResult result = query("select set(a) as a from large;");
    assertArrayEquals(new String[] { "a", "b" }, (String[]) result.getValues().get(0)[0]);
  }

  @Test
  public void testCase() throws Exception {
    QueryResult result = query("select case when a = 'abc' then b else c end as a from table;");
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { 6.7 }, result.getValues().get(1));
  }

  @Test
  public void testDates() throws Exception {
    QueryResult result = query("select a from dates;");
    assertArrayEquals(new Object[] { sdf.parse("20170101") }, result.getValues().get(0));
    assertArrayEquals(new Object[] { sdf.parse("20170201") }, result.getValues().get(1));
  }

  @Test
  public void testDateFunctions1() throws Exception {
    QueryResult result = query("select "
        + "a > date('2017-01-15') as x, "
        + "a > date('2017-01-15 00:00:00') as y, "
        + "date('20170115') as z from dates;");
    assertArrayEquals(new Object[] { false, false, null }, result.getValues().get(0));
    assertArrayEquals(new Object[] { true, true, null }, result.getValues().get(1));
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
    assertArrayEquals(new Object[] {
        sdf.parse("20180101"),
        sdf.parse("20170201"),
        sdf.parse("20170108"),
        sdf.parse("20170102"),
        sdf2.parse("20170101 010000"),
        sdf2.parse("20170101 000100"),
        sdf2.parse("20170101 000001") }, result.getValues().get(0));
  }

  @Test
  public void testEnumInFunc() throws Exception {
    QueryResult result = query("select length(a) as l from enums;");
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(0));
    assertArrayEquals(new Object[] { 1L }, result.getValues().get(1));
  }

  @Test
  public void testEnumInBinaryExpr() throws Exception {
    QueryResult result = query("select a + 'z' as a from enums;");
    assertArrayEquals(new Object[] { "xz" }, result.getValues().get(0));
    assertArrayEquals(new Object[] { "yz" }, result.getValues().get(1));
  }

  @Test
  public void testEnumInAggr() throws Exception {
    QueryResult result = query("select max(a) as a from enums;");
    assertArrayEquals(new Object[] { "y" }, result.getValues().get(0));
  }

  @Test
  public void testAggrInAggr() throws Exception {
    ErrorResult e = error("select sum(sum(b)) from large;");
    assertError(NotAggrTableException.class, "[11, 14]: Not an aggregation table 'sum'.", e);
  }

  @Test
  public void testNonKeyOutsideOfAggr() throws Exception {
    ErrorResult e = error("select b, sum(c) from large group by a;");
    assertError(ModelException.class, "[7, 8]: Column 'b' not found in table.", e);
  }

  @Test
  public void testGroupByInconsistentAggr() throws Exception {
    ErrorResult e = error("select sum(b) + b from large group by a;");
    assertError(ModelException.class, "[16, 17]: Column 'b' not found in table.", e);
  }

  @Test
  public void testInconsistentAggr() throws Exception {
    ErrorResult e = error("select sum(b) + b from large;");
    assertError(ModelException.class, "[16, 17]: Column 'b' not found in table.", e);
  }

  @Test
  public void testWrongArgNumber() throws Exception {
    ErrorResult e = error("select length(a, 1) from table;");
    assertError(ModelException.class, "[7, 13]: Expected 1 columns for function 'length' but got 2.", e);
  }

  @Test
  public void testInvalidAggrArg() throws Exception {
    ErrorResult e = error("select max(a = 'x') from table;");
    assertError(ModelException.class, "[7, 10]: Invalid argument type 'boolean' for aggregator 'max'.", e);
  }

  @Test
  public void testInvalidJoinOn() throws Exception {
    ErrorResult e = error("select * from left inner join right on a > x;");
    assertError(ModelException.class,
        "[40, 41]: Only 'and' and '=' binary expressions are allowed in the 'on' expression of joins, not '>'.", e);
  }

  @Test
  public void testDuplicateColumnNames() throws Exception {
    ErrorResult e = error("select a, b as a from table;");
    assertError(ModelException.class, "[15, 16]: Duplicate column name 'a'.", e);
  }

  @Test
  public void testExpressionInGroupByNotNamed() throws Exception {
    ErrorResult e = error("select count(1) from table group by a + a;");
    assertError(ModelException.class, "[37, 38]: Expression in group by must be named: '(a + a)'.", e);
  }
}
