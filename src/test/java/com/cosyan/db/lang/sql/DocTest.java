/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;

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

  private void runDocFile(String fileName) throws IOException {
    System.out.print("Testing doc " + fileName + ": ");
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.equals("<!-- RUN -->")) {
        execute(readCode(reader));
      } else if (line.equals("<!-- TEST -->")) {
        QueryResult result = query(readCode(reader));
        assertEquals(readCode(reader), result.prettyPrint());
        System.out.print(".");
      } else if (line.equals("<!-- ERROR -->")) {
        ErrorResult error = error(readCode(reader));
        assertEquals(readCode(reader).trim(), error.getError().getMessage().trim());
        System.out.print(".");
      }
    }
    reader.close();
    System.out.println("\nDoc test " + fileName + " passed.");
  }

  @Test
  public void testForeignKeys() throws Exception {
    setUp();
    runDocFile("doc/rules/31_foreign_keys.md");
  }

  @Test
  public void testReverseForeignKeys() throws Exception {
    setUp();
    runDocFile("doc/rules/32_reverse_foreign_keys.md");
  }

  @Test
  public void testDateTimes() throws Exception {
    setUp();
    runDocFile("doc/rules/33_date_time.md");
  }

  @Test
  public void testNetworks() throws Exception {
    setUp();
    runDocFile("doc/rules/34_networks.md");
  }
}
