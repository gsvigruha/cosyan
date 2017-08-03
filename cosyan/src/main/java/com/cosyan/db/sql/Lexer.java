package com.cosyan.db.sql;

import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Tokens.Token;

import com.google.common.collect.ImmutableList;

public class Lexer {

  public ImmutableList<Token> tokenize(String sql) throws ParserException {
    ImmutableList.Builder<Token> builder = ImmutableList.builder();
    String ident = "";
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (Tokens.DELIMITERS.contains(c)) {
        if (ident.matches(Tokens.IDENT)) {
          builder.add(new Token(ident));
        } else if (!ident.isEmpty()) {
          throw new ParserException("Invalid identifier '" + ident + "' (" + i + ").");
        }
        ident = "";
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
        ident = ident + c;
      }
    }
    return builder.build();
  }
}
