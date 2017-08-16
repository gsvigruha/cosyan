package com.cosyan.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class Tokens {

  public static String SELECT = "select";

  public static String DISTINCT = "distinct";

  public static String FROM = "from";

  public static String WHERE = "where";

  public static String GROUP = "group";

  public static String ORDER = "order";

  public static String BY = "by";

  public static String ASC = "asc";

  public static String DESC = "desc";

  public static String HAVING = "having";

  public static String AS = "as";

  private static String IDENT_COMP = "[a-z][a-z0-9_]*";

  public static String IDENT = IDENT_COMP + "(\\." + IDENT_COMP + ")*";

  public static String STRING_LITERAL = "'.*'";

  public static String LONG_LITERAL = "[0-9]+";

  public static String DOUBLE_LITERAL = "[0-9]+\\.[0-9]+";

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

  public static String SUM = "sum";

  public static ImmutableSet<Character> DELIMITERS = ImmutableSet.<Character>builder()
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

  public static ImmutableList<ImmutableSet<String>> BINARY_OPERATORS_PRECEDENCE = ImmutableList
      .<ImmutableSet<String>>builder()
      .add(ImmutableSet.of(ASC, DESC))
      .add(ImmutableSet.of(OR))
      .add(ImmutableSet.of(AND))
      .add(ImmutableSet.of(XOR))
      .add(ImmutableSet.of(NOT))
      .add(ImmutableSet.of(
          String.valueOf(EQ),
          String.valueOf(LESS),
          String.valueOf(GREATER),
          LEQ,
          GEQ))
      .add(ImmutableSet.of(
          String.valueOf(PLUS),
          String.valueOf(MINUS),
          String.valueOf(DIV),
          String.valueOf(ASTERISK),
          String.valueOf(MOD)))
      .build();

  private static ImmutableSet.Builder<String> binOpsBuilder = ImmutableSet.builder();
  static {
    for (ImmutableSet<String> ops : BINARY_OPERATORS_PRECEDENCE) {
      binOpsBuilder.addAll(ops);
    }
  }
  public static ImmutableSet<String> BINARY_OPERATORS = binOpsBuilder.build();

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

    public boolean is(String s) {
      return string.equals(s);
    }
  }
}
