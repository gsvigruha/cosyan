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
package com.cosyan.db.logic;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.model.Ident;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;

public class PredicateHelperTest {

  private Parser parser = new Parser();
  private Lexer lexer = new Lexer();

  private void assertClause(String sql, VariableEquals... clauses) throws ParserException {
    assertEquals(ImmutableList.copyOf(clauses),
        PredicateHelper.extractClauses(parser.parseExpression(lexer.tokenize(sql))));
  }

  @Test
  public void testExtractClauses() throws ParserException {
    assertClause("a = 1 and b = 2;",
        new VariableEquals(new Ident("a"), 1L),
        new VariableEquals(new Ident("b"), 2L));
    assertClause("a = 1 and 2 = b;",
        new VariableEquals(new Ident("a"), 1L),
        new VariableEquals(new Ident("b"), 2L));
    assertClause("a = 1 or b = 2;");
    assertClause("a = 1 and (b = 2 and c = 3);",
        new VariableEquals(new Ident("a"), 1L),
        new VariableEquals(new Ident("b"), 2L),
        new VariableEquals(new Ident("c"), 3L));
    assertClause("(a > 1 or b = 2) and c = 3;",
        new VariableEquals(new Ident("c"), 3L));
  }
}
