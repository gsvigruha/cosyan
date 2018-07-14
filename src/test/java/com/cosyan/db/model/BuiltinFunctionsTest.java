package com.cosyan.db.model;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;

public class BuiltinFunctionsTest extends UnitTestBase {

  @BeforeClass
  public static void setUp() throws Exception {
    UnitTestBase.setUp();
    execute("create table t (a varchar);");
    execute("insert into t values ('abcABC');");
  }

  private void assertResult(String expr, Object result)
      throws ModelException, ConfigException, ParserException, IOException {
    QueryResult r = query("select " + expr + " as r from t;");
    assertEquals(result, r.getValues().get(0)[0]);
  }

  @Test
  public void testStringFunctions() throws Exception {
    assertResult("length(a)", 6L);
    assertResult("a.length()", 6L);
    assertResult("upper(a)", "ABCABC");
    assertResult("lower(a)", "abcabc");
    assertResult("substr(a, 1, 4)", "bcAB");
    assertResult("a.substr(2, 1)", "c");
    assertResult("contains(a, 'bc')", true);
    assertResult("a.contains('BC')", true);
    assertResult("a.contains('XY')", false);
    assertResult("matches(a, '.*')", true);
    assertResult("matches(a, '[0-9]*')", false);
    assertResult("replace(a, 'b', 'x')", "axcABC");
    assertResult("a.replace('c', 'x')", "abxABC");
    assertResult("trim(' abc ')", "abc");
    assertResult("'xyz'.concat('xyz')", "xyzxyz");
    assertResult("index_of('xyz', 'x')", 0L);
    assertResult("last_index_of('aaa', 'a')", 2L);
  }

  @Test
  public void testMathFunctions() throws Exception {
    assertResult("pow(3.0, 2.0)", 9.0);
    assertResult("exp(1.0)", Math.E);
    assertResult("log2(8.0)", 3.0);
    assertResult("loge(1.0)", 0.0);
    assertResult("log10(100.0)", 2.0);
    assertResult("log(81.0, 3.0)", 4.0);
    assertResult("round(1.5)", 2L);
    assertResult("ceil(1.1)", 2L);
    assertResult("floor(1.7)", 1L);
    assertResult("abs(-10.0)", 10.0);
    assertResult("sin(0.0)", 0.0);
    assertResult("sinh(0.0)", 0.0);
    assertResult("cos(0.0)", 1.0);
    assertResult("cosh(0.0)", 1.0);
    assertResult("tan(0.0)", 0.0);
    assertResult("tanh(0.0)", 0.0);
  }

  @Test
  public void testImplicitLongToDoubleConversion() throws Exception {
    assertResult("pow(3, 2)", 9.0);
    assertResult("abs(-1)", 1.0);
  }
}
