package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.Tokens.FloatToken;
import com.cosyan.db.lang.sql.Tokens.IntToken;
import com.cosyan.db.lang.sql.Tokens.StringToken;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;

public class LexerTest {

  private Lexer lexer = new Lexer();

  @Test
  public void testBinaryExpressions() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new Token("b"), new Token(";")),
        lexer.tokens("a+b;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<"), new Token("abc"), new Token(";")),
        lexer.tokens("xyz<abc;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<="), new Token("abc"), new Token(";")),
        lexer.tokens("xyz<=abc;"));
  }

  @Test
  public void testLiterals() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokens("a+'b';"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokens("\"xyz\"+b;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new IntToken("100"), new Token(";")),
        lexer.tokens("a<100;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new FloatToken("1.5"), new Token(";")),
        lexer.tokens("a<1.5;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new FloatToken("-1.5"), new Token(";")),
        lexer.tokens("a<-1.5;"));
    assertEquals(ImmutableList.of(new IntToken("3"), new Token("+"), new IntToken("-1"), new Token(";")),
        lexer.tokens("3+-1;"));
  }

  @Test
  public void testWhitespaces() throws ParserException {
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new Token("b"), new Token(";")),
        lexer.tokens("a + b;"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("<"), new Token("abc"), new Token(";")),
        lexer.tokens("xyz     < \nabc;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokens("a+  'b';"));
    assertEquals(ImmutableList.of(new Token("xyz"), new Token("+"), new StringToken("b"), new Token(";")),
        lexer.tokens("\"xyz\"  +\nb;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new IntToken("100"), new Token(";")),
        lexer.tokens("a< 100 ;"));
    assertEquals(ImmutableList.of(new Token("a"), new Token("<"), new FloatToken("1.5"), new Token(";")),
        lexer.tokens("a < 1.5;"));
  }
}
