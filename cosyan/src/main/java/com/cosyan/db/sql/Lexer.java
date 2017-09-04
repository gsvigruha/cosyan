package com.cosyan.db.sql;

import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Tokens.FloatToken;
import com.cosyan.db.sql.Tokens.IdentToken;
import com.cosyan.db.sql.Tokens.IntToken;
import com.cosyan.db.sql.Tokens.StringToken;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;

public class Lexer {

  private static final int STATE_DEFAULT = 0;
  private static final int STATE_IN_SINGLE_QUOTE = 1;
  private static final int STATE_IN_DOUBLE_QUOTE = 2;
  private static final int STATE_NUMBER_LITERAL = 3;
  private static final int STATE_FLOAT_LITERAL = 4;
  private static final int STATE_IDENT = 5;

  public ImmutableList<Token> tokenize(String sql) throws ParserException {
    int state = STATE_DEFAULT;
    ImmutableList.Builder<Token> builder = ImmutableList.builder();
    String literal = "";
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (state == STATE_IN_SINGLE_QUOTE) {
        if (c == Tokens.SINGLE_QUOTE) {
          builder.add(new StringToken(literal));
          state = STATE_DEFAULT;
          literal = "";
        } else {
          literal = literal + c;
        }
      } else if (state == STATE_IN_DOUBLE_QUOTE) {
        if (c == Tokens.DOUBLE_QUOTE) {
          builder.add(new StringToken(literal));
          state = STATE_DEFAULT;
          literal = "";
        } else {
          literal = literal + c;
        }
      } else if (state == STATE_NUMBER_LITERAL) {
        if (c == Tokens.DOT) {
          state = STATE_FLOAT_LITERAL;
          literal = literal + c;
        } else if (Tokens.DELIMITERS.contains(c)) {
          builder.add(new IntToken(Long.valueOf(literal).toString()));
          state = STATE_DEFAULT;
          literal = "";
          i--;
        } else {
          literal = literal + c;
        }
      } else if (state == STATE_FLOAT_LITERAL) {
        if (Tokens.DELIMITERS.contains(c)) {
          builder.add(new FloatToken(Double.valueOf(literal).toString()));
          state = STATE_DEFAULT;
          literal = "";
          i--;
        } else {
          literal = literal + c;
        }
      } else if (state == STATE_IDENT) {
        if (Tokens.DELIMITERS.contains(c)) {
          builder.add(new IdentToken(literal));
          state = STATE_DEFAULT;
          literal = "";
          i--;
        } else {
          literal = literal + c;
        }
      } else if (state == STATE_DEFAULT) {
        if (Tokens.DELIMITERS.contains(c)) {
          if (c == Tokens.LESS && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
            builder.add(new Token(Tokens.LEQ));
            i++;
          } else if (c == Tokens.GREATER && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
            builder.add(new Token(Tokens.GEQ));
            i++;
          } else if (!Tokens.WHITESPACE.contains(c)) {
            builder.add(new Token(String.valueOf(c)));
          }
        } else if (c == Tokens.SINGLE_QUOTE) {
          state = STATE_IN_SINGLE_QUOTE;
        } else if (c == Tokens.DOUBLE_QUOTE) {
          state = STATE_IN_DOUBLE_QUOTE;
        } else {
          if (c >= '0' && c <= '9') {
            state = STATE_NUMBER_LITERAL;
          } else if (c >= 'a' && c <= 'z') {
            state = STATE_IDENT;
          }
          literal = literal + c;
        }
      }
    }
    return builder.build();
  }
}
