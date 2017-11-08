package com.cosyan.db.lang.sql;

import java.util.ArrayList;

import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.Tokens.FloatToken;
import com.cosyan.db.lang.sql.Tokens.IdentToken;
import com.cosyan.db.lang.sql.Tokens.IntToken;
import com.cosyan.db.lang.sql.Tokens.StringToken;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class Lexer {

  private static final int STATE_DEFAULT = 0;
  private static final int STATE_IN_SINGLE_QUOTE = 1;
  private static final int STATE_IN_DOUBLE_QUOTE = 2;
  private static final int STATE_NUMBER_LITERAL = 3;
  private static final int STATE_FLOAT_LITERAL = 4;
  private static final int STATE_IDENT = 5;

  public PeekingIterator<Token> tokenize(String sql) throws ParserException {
    return Iterators.peekingIterator(tokens(sql).iterator());
  }

  ImmutableList<Token> tokens(String sql) throws ParserException {
    int state = STATE_DEFAULT;
    ArrayList<Token> builder = new ArrayList<>();
    int literalStartIndex = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (state == STATE_IN_SINGLE_QUOTE) {
        if (c == Tokens.SINGLE_QUOTE) {
          builder.add(new StringToken(sql.substring(literalStartIndex + 1, i)));
          state = STATE_DEFAULT;
          literalStartIndex = i;
        }
      } else if (state == STATE_IN_DOUBLE_QUOTE) {
        if (c == Tokens.DOUBLE_QUOTE) {
          builder.add(new StringToken(sql.substring(literalStartIndex + 1, i)));
          state = STATE_DEFAULT;
          literalStartIndex = i;
        }
      } else if (state == STATE_NUMBER_LITERAL) {
        if (c == Tokens.DOT) {
          state = STATE_FLOAT_LITERAL;
        } else if (Tokens.isDelimiter(c)) {
          builder.add(new IntToken(sql.substring(literalStartIndex, i)));
          state = STATE_DEFAULT;
          literalStartIndex = i;
          i--;
        } else {
          if (!Tokens.isDigit(c)) {
            throw new ParserException("Wrong number.");
          }
        }
      } else if (state == STATE_FLOAT_LITERAL) {
        if (Tokens.isDelimiter(c)) {
          builder.add(new FloatToken(sql.substring(literalStartIndex, i)));
          state = STATE_DEFAULT;
          literalStartIndex = i;
          i--;
        } else {
          if (!Tokens.isDigit(c)) {
            throw new ParserException("Wrong number.");
          }
        }
      } else if (state == STATE_IDENT) {
        if (Tokens.isDelimiter(c) || c == Tokens.DOT) {
          builder.add(new IdentToken(sql.substring(literalStartIndex, i)));
          state = STATE_DEFAULT;
          literalStartIndex = i;
          i--;
        } else {
          if (!(Tokens.isDigit(c) || Tokens.isLowerCaseLetter(c) || Tokens.isUpperCaseLetter(c) || c == '_')) {
            throw new ParserException("Wrong ident.");
          }
        }
      } else if (state == STATE_DEFAULT) {
        if (Tokens.isDigit(c) || (c == Tokens.MINUS && i < sql.length() - 1 && Tokens.isDigit(sql.charAt(i + 1)))) {
          state = STATE_NUMBER_LITERAL;
          literalStartIndex = i;
        } else if (Tokens.isDelimiter(c)) {
          if (c == Tokens.LESS && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
            builder.add(new Token(Tokens.LEQ));
            i++;
          } else if (c == Tokens.GREATER && i < sql.length() - 1 && sql.charAt(i + 1) == Tokens.EQ) {
            builder.add(new Token(Tokens.GEQ));
            i++;
          } else if (!Tokens.isWhitespace(c)) {
            builder.add(new Token(String.valueOf(c)));
          }
          literalStartIndex = i;
        } else if (c == Tokens.SINGLE_QUOTE) {
          state = STATE_IN_SINGLE_QUOTE;
          literalStartIndex = i;
        } else if (c == Tokens.DOUBLE_QUOTE) {
          state = STATE_IN_DOUBLE_QUOTE;
          literalStartIndex = i;
        } else if (Tokens.isLowerCaseLetter(c) || c == Tokens.DOT) {
          if (c == Tokens.DOT) {
            builder.add(new Token(String.valueOf(c)));
            i++;
          }
          state = STATE_IDENT;
          literalStartIndex = i;
        } else {
          throw new ParserException("Syntax error.");
        }
      }
    }
    return ImmutableList.copyOf(builder);
  }
}
