package com.cosyan.db.logic;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.sql.Lexer;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableList;

public class PredicateHelperTest {

  private Parser parser = new Parser();
  private Lexer lexer = new Lexer();

  private void assertClause(String sql, VariableEquals... clauses) throws ParserException {
    assertEquals(ImmutableList.copyOf(clauses),
        PredicateHelper.extractClauses(parser.parseExpression(lexer.tokenize(sql), 0)));
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
