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

  @Test
  public void testCSVImportNoHeader() throws IOException, ParseException {
    String csv = getClass().getClassLoader().getResource("test_2.csv").getFile();
    execute("create table t4(a varchar, b integer);");
    execute("import from csv '" + csv + "' into t4;");
    QueryResult r = query("select * from t4;");
    assertValues(new Object[][] { { "abc", 1L } }, r);
  }

  @Test
  public void testCSVImportDifferentColumnOrder() throws IOException, ParseException {
    String csv = getClass().getClassLoader().getResource("test_4.csv").getFile();
    execute("create table t5(a varchar, b integer, c float);");
    execute("import from csv '" + csv + "' into t5 with header;");
    QueryResult r = query("select * from t5;");
    assertValues(new Object[][] { { "abc", 1L, 1.0 } }, r);
  }

  @Test
  public void testCSVImportCimmitAfterNRecords() throws IOException, ParseException {
    String csv = getClass().getClassLoader().getResource("test_5.csv").getFile();
    execute("create table t6(a varchar);");
    execute("import from csv '" + csv + "' into t6 commit 1;");
    QueryResult r = query("select * from t6;");
    assertValues(new Object[][] { { "abc" }, { "abc" }, { "abc" } }, r);
  }
}
