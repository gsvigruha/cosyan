package com.cosyan.db.lang.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.Data;

public class Tokens {

  public static String CREATE = "create";

  public static String DROP = "drop";

  public static String ALTER = "alter";

  public static String ADD = "add";

  public static String MODIFY = "modify";

  public static String LOOKUP = "lookup";

  public static String LOG = "log";

  public static String TABLE = "table";

  public static String INDEX = "index";

  public static String USER = "user";

  public static String IDENTIFIED = "identified";

  public static String PARTITION = "partition";

  public static String GRANT = "grant";

  public static String TO = "to";

  public static String WITH = "with";

  public static String OPTION = "option";

  public static String ALL = "all";

  public static String VARCHAR = "varchar";

  public static String INTEGER = "integer";

  public static String FLOAT = "float";

  public static String BOOLEAN = "boolean";

  public static String TIMESTAMP = "timestamp";

  public static String DT = "dt";

  public static String TRUE = "true";

  public static String FALSE = "false";

  public static String UNIQUE = "unique";

  public static String IMMUTABLE = "immutable";

  public static String PRIMARY = "primary";

  public static String FOREIGN = "foreign";

  public static String KEY = "key";

  public static String REF = "ref";

  public static String REFERENCES = "references";

  public static String REVERSE = "reverse";

  public static String CONSTRAINT = "constraint";

  public static String CHECK = "check";

  public static String INSERT = "insert";

  public static String INTO = "into";

  public static String VALUES = "values";

  public static String DELETE = "delete";

  public static String UPDATE = "update";

  public static String SET = "set";

  public static String SELECT = "select";

  public static String DISTINCT = "distinct";

  public static String FROM = "from";

  public static String INNER = "inner";

  public static String LEFT = "left";

  public static String RIGHT = "right";

  public static String FULL = "full";

  public static String OUTER = "outer";

  public static String JOIN = "join";

  public static String ON = "on";

  public static String WHERE = "where";

  public static String GROUP = "group";

  public static String ORDER = "order";

  public static String BY = "by";

  public static String ASC = "asc";

  public static String DESC = "desc";

  public static String HAVING = "having";

  public static String AS = "as";

  public static String CASE = "case";

  public static String WHEN = "when";

  public static String THEN = "then";

  public static String ELSE = "else";

  public static String END = "end";

  public static String ID = "id";

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

  public static char SINGLE_QUOTE = '\'';

  public static char DOUBLE_QUOTE = '"';

  public static char DOT = '.';

  public static String LEQ = "<=";

  public static String GEQ = ">=";

  public static String IS = "is";

  public static String NULL = "null";

  public static String NOT = "not";

  public static String AND = "and";

  public static String OR = "or";

  public static String XOR = "xor";

  public static String IMPL = "impl";

  public static String COUNT = "count";

  public static String AVG = "avg";

  public static String MIN = "min";

  public static String MAX = "max";

  public static String SUM = "sum";

  public static boolean isDelimiter(char c) {
    return c == SPACE ||
        c == TAB ||
        c == NEWLINE ||
        c == CARBACK ||
        c == COMMA ||
        c == COMMA_COLON ||
        c == PLUS ||
        c == MINUS ||
        c == ASTERISK ||
        c == DIV ||
        c == MOD ||
        c == EQ ||
        c == LESS ||
        c == GREATER ||
        c == PARENT_CLOSED ||
        c == PARENT_OPEN;
  }

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
      .add(ImmutableSet.of(IMPL))
      .add(ImmutableSet.of(IS))
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

  public static boolean isWhitespace(char c) {
    return c == SPACE || c == TAB || c == NEWLINE || c == CARBACK;
  }

  public static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  public static boolean isLowerCaseLetter(char c) {
    return c >= 'a' && c <= 'z';
  }

  public static boolean isUpperCaseLetter(char c) {
    return c >= 'A' && c <= 'Z';
  }

  @Data
  public static class Loc {
    private final int start;
    private final int end;

    public static Loc interval(Loc start, Loc end) {
      return new Loc(start.start, end.end);
    }
  }

  @Data
  public static class Token {
    private final String string;
    private final Loc loc;

    public Token(String string, Loc loc) {
      this.string = string;
      this.loc = loc;
    }

    @Override
    public String toString() {
      return string;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Token) {
        return string.equals(((Token) other).string);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return string.hashCode();
    }

    public boolean is(char c) {
      return string.length() == 1 && string.charAt(0) == c;
    }

    public boolean is(String s) {
      return string.equals(s);
    }

    public boolean isString() {
      return false;
    }

    public boolean isFloat() {
      return false;
    }

    public boolean isInt() {
      return false;
    }

    public boolean isBoolean() {
      return false;
    }

    public boolean isIdent() {
      return false;
    }
  }

  public static class StringToken extends Token {

    public StringToken(String original, int start, int end) {
      super(original.substring(start, end), new Loc(start, end));
    }

    public StringToken(String string, Loc loc) {
      super(string, loc);
    }

    @Override
    public boolean isString() {
      return true;
    }
  }

  public static class IntToken extends Token {

    public IntToken(String original, int start, int end) {
      super(original.substring(start, end), new Loc(start, end));
    }

    public IntToken(String string, Loc loc) {
      super(string, loc);
    }

    @Override
    public boolean isInt() {
      return true;
    }
  }

  public static class FloatToken extends Token {

    public FloatToken(String original, int start, int end) {
      super(original.substring(start, end), new Loc(start, end));
    }

    public FloatToken(String string, Loc loc) {
      super(string, loc);
    }

    @Override
    public boolean isFloat() {
      return true;
    }
  }

  public static class BooleanToken extends Token {
    public BooleanToken(String string, Loc loc) {
      super(string, loc);
    }

    @Override
    public boolean isBoolean() {
      return true;
    }
  }

  public static class IdentToken extends Token {
    public IdentToken(String string, Loc loc) {
      super(string, loc);
    }

    @Override
    public boolean isIdent() {
      return true;
    }
  }
}
