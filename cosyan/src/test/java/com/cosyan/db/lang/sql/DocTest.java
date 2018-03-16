package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;

public class DocTest extends UnitTestBase {

  private String readCode(BufferedReader reader) throws IOException {
    assertEquals("```", reader.readLine());
    String line;
    StringBuilder sb = new StringBuilder();
    while (!(line = reader.readLine()).equals("```")) {
      sb.append(line).append("\n");
    }
    return sb.toString();
  }

  @Test
  public void testDocs() throws IOException {
    File file = new File(getClass().getClassLoader().getResource("doc/rules/foreign_keys.md").getFile());
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.equals("<!-- RUN -->")) {
        execute(readCode(reader));
      } else if (line.equals("<!-- TEST -->")) {
        QueryResult result = query(readCode(reader));
        assertEquals(readCode(reader), result.prettyPrint());
      } else if (line.equals("<!-- ERROR -->")) {
        ErrorResult error = error(readCode(reader));
        assertEquals(readCode(reader).trim(), error.getError().getMessage().trim());
      }
    }
    reader.close();
  }
}
