package com.cosyan.db.model;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.io.IOTestUtil.DummyMaterializedTableMeta;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.Compiler;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.google.common.collect.ImmutableMap;

public class BuiltinFunctionsTest {
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
            "a", new BasicColumn(0, "a", DataTypes.StringType)),
        new Object[][] {
            new Object[] { "abcABC" } }));
  }

  private void assertResult(String expr, Object result)
      throws ModelException, ConfigException, ParserException, IOException {
    ExposedTableReader reader = compiler.query(parser.parse("select " + expr + " as r from table;")).reader();
    assertEquals(ImmutableMap.of("r", result), reader.readColumns());
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
  }
}
