package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Tokens.FloatToken;
import com.cosyan.db.sql.Tokens.IntToken;
import com.cosyan.db.sql.Tokens.StringToken;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;

public class LexerTest {

  private Lexer lexer = new Lexer();

  @Test
  public void testBinaryExpressions() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new Token("b"), new Token(";")),
        lexer.tokenize("a+b;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<"), new Token("abc"), new Token(";")),
        lexer.tokenize("xyz<abc;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<="), new Token("abc"), new Token(";")),
        lexer.tokenize("xyz<=abc;"));
  }

  @Test
  public void testLiterals() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokenize("a+'b';"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokenize("\"xyz\"+b;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new IntToken("100"), new Token(";")),
        lexer.tokenize("a<100;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new FloatToken("1.5"), new Token(";")),
        lexer.tokenize("a<1.5;"));
  }

  @Test
  public void testWhitespaces() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new Token("b"), new Token(";")),
        lexer.tokenize("a + b;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<"), new Token("abc"), new Token(";")),
        lexer.tokenize("xyz     < \nabc;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokenize("a+  'b';"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokenize("\"xyz\"  +\nb;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new IntToken("100"), new Token(";")),
        lexer.tokenize("a< 100 ;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new FloatToken("1.5"), new Token(";")),
        lexer.tokenize("a < 1.5;"));
  }
}
