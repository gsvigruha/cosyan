package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.cosyan.db.UnitTestBase;

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
}
