package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.model.DateFunctions;

public class CSVStatementTest extends UnitTestBase {

  private void assertFileContent(String f1, String f2) throws IOException {
    assertEquals(
        FileUtils.readFileToString(new File(f1), Charset.defaultCharset()).trim(),
        FileUtils.readFileToString(new File(f2), Charset.defaultCharset()).trim());
  }

  @Test
  public void testCSVExport() throws IOException {
    String output = config.dataDir() + File.separator + "output_1.csv";
    execute("create table t1(a varchar, b integer, c float, d boolean, e timestamp, f enum('x', 'y'));");
    execute("insert into t1 values ('abc', 1, 2.3, true, dt '2018-01-01', 'x');");
    execute("export into csv '" + output + "' (select * from t1);");
    assertFileContent(output, getClass().getClassLoader().getResource("test_1.csv").getFile());
  }

  @Test
  public void testCSVImport() throws IOException, ParseException {
    String csv = getClass().getClassLoader().getResource("test_1.csv").getFile();
    execute("create table t2(a varchar, b integer, c float, d boolean, e timestamp, f enum('x', 'y'));");
    execute("import from csv '" + csv + "' into t2 with header;");
    QueryResult r = query("select * from t2;");
    assertValues(new Object[][] { { "abc", 1L, 2.3, true, DateFunctions.sdf2.parse("2018-01-01"), "x" } }, r);
  }

  @Test
  public void testCSVExportWithNull() throws IOException {
    String output = config.dataDir() + File.separator + "output_3.csv";
    execute("create table t3(a varchar, b integer);");
    execute("insert into t3 (b) values (1);");
    execute("export into csv '" + output + "' (select * from t3);");
    assertFileContent(output, getClass().getClassLoader().getResource("test_3.csv").getFile());
  }
}
