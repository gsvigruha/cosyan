package com.cosyan.db.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class Tokens {

  public static String SELECT = "select";

  public static String DISTINCT = "distinct";

  public static String FROM = "from";

  public static String WHERE = "where";

  private static String IDENT_COMP = "[a-z][a-z0-9_]*";

  public static String IDENT = IDENT_COMP + "(\\." + IDENT_COMP + ")*";

  public static char SPACE = ' ';

  public static char TAB = '\t';

  public static char NEWLINE = '\n';

  public static char CARBACK = '\r';
  
  public static char COMMA = ',';
  
  public static char COMMA_COLON = ';';

  public static char PLUS = '+';

  public static char MINUS = '-';

  public static char ASTERISK = '*';

  public static char DIV = '/';

  public static char MOD = '%';

  public static char EQ = '=';

  public static char LESS = '<';

  public static char GREATER = '>';

  public static char PARENT_OPEN = '(';

  public static char PARENT_CLOSED = ')';

  public static String LEQ = "<=";

  public static String GEQ = ">=";
  
  public static String NOT = "not";
  
  public static String AND = "and";
  
  public static String OR = "or";
  
  public static String XOR = "xor";

  public static String COUNT = "count";

  public static String AVG = "avg";

  public static String MIN = "min";

  public static String MAX = "max";

  public static ImmutableSet<Character> DELIMITERS = ImmutableSet.<Character> builder()
      .add(SPACE)
      .add(TAB)
      .add(NEWLINE)
      .add(CARBACK)
      .add(COMMA)
      .add(COMMA_COLON)
      .add(PLUS)
      .add(MINUS)
      .add(ASTERISK)
      .add(DIV)
      .add(MOD)
      .add(EQ)
      .add(LESS)
      .add(GREATER)
      .add(PARENT_OPEN)
      .add(PARENT_CLOSED)
      .build();

  public static ImmutableSet<String> KEYWORDS = ImmutableSet.of(
      SELECT,
      DISTINCT,
      FROM,
      WHERE);
  
  public static ImmutableSet<String> AGGREGATORS = ImmutableSet.<String> builder()
      .add(COUNT)
      .add(AVG)
      .add(MIN)
      .add(MAX)
      .build();
  
  public static ImmutableMap<String, Integer> BINARY_BOOL_OPERATORS = ImmutableMap.<String, Integer> builder()
      .put(AND, 1)
      .put(OR, 2)
      .put(XOR, 3)
      .build();
  
  public static ImmutableSet<String> BINARY_LOGIC_OPERATORS = ImmutableSet.<String> builder()
      .add(String.valueOf(EQ))
      .add(String.valueOf(LESS))
      .add(String.valueOf(GREATER))
      .add(LEQ)
      .add(GEQ)
      .build();

  public static ImmutableSet<Character> WHITESPACE = ImmutableSet.of(
    SPACE,
    TAB,
    NEWLINE,
    CARBACK);

  public static class Token {
    private final String string;

    public Token(String string) {
      this.string = string;
    }

    public String getString() {
      return string;
    }

    @Override
    public String toString() {
      return string;
    }

    public boolean is(char c) {
      return string.equals(String.valueOf(c));
    }
  }
}
