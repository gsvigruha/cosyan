package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.model.DateFunctions;

public class CSVStatementTest extends UnitTestBase {

  @Test
  public void testCSVExport() throws IOException {
    String output = config.confDir() + File.separator + "output.csv";
    execute("create table t1(a varchar, b integer, c float, d boolean, e timestamp, f enum('x', 'y'));");
    execute("insert into t1 values ('abc', 1, 2.3, true, dt '2018-01-01', 'x');");
    execute("export into csv '" + output + "' (select * from t1);");
    assertTrue(FileUtils.contentEquals(new File(output),
        new File(getClass().getClassLoader().getResource("test_1.csv").getFile())));
  }

  @Test
  public void testCSVImport() throws IOException, ParseException {
    String csv = getClass().getClassLoader().getResource("test_1.csv").getFile();
    execute("create table t2(a varchar, b integer, c float, d boolean, e timestamp, f enum('x', 'y'));");
    execute("import from csv '" + csv + "' into t2 true;");
    QueryResult r = query("select * from t2;");
    assertValues(new Object[][] { { "abc", 1L, 2.3, true, DateFunctions.sdf2.parse("2018-01-01"), "x" } }, r);
  }
}
