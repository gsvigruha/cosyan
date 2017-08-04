package com.cosyan.db.sql;

import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Tokens.Token;

import com.google.common.collect.ImmutableList;

public class Lexer {

  public ImmutableList<Token> tokenize(String sql) throws ParserException {
    ImmutableList.Builder<Token> builder = ImmutableList.builder();
    String literal = "";
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (Tokens.DELIMITERS.contains(c)) {
        if (literal.matches(Tokens.IDENT)) {
          builder.add(new Token(literal));
        } else if (literal.matches(Tokens.STRING_LITERAL)) {
          builder.add(new Token(literal));
        } else if (literal.matches(Tokens.LONG_LITERAL)) {
          builder.add(new Token(literal));
        } else if (literal.matches(Tokens.DOUBLE_LITERAL)) {
          builder.add(new Token(literal));
        } else if (!literal.isEmpty()) {
          throw new ParserException("Invalid literal '" + literal + "' (" + i + ").");
        }
        literal = "";
        if (c == Tokens.LESS && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
          builder.add(new Token(Tokens.LEQ));
          i++;
        } else if (c == Tokens.GREATER && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
          builder.add(new Token(Tokens.GEQ));
          i++;
        } else if (!Tokens.WHITESPACE.contains(c)) {
          builder.add(new Token(String.valueOf(c)));
        }
      } else {
        literal = literal + c;
      }
    }
    return builder.build();
  }
}
