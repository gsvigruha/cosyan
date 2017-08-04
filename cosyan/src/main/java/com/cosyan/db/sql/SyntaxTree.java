package com.cosyan.db.sql;

import java.util.Optional;

import com.cosyan.db.sql.Tokens.Token;

import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class SyntaxTree {

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class Node {

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Ident extends Node {
    private final String string;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Expression extends Node {

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Select extends Node {
    private final ImmutableList<Expression> columns;
    private final Table table;
    private final Optional<Expression> where;
  }

  public static class Table extends Node {

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Ident ident;
    private final Expression expr;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IdentExpression extends Expression {
    private final Ident ident;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression {
    private final String val;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression {
    private final long val;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression {
    private final Double val;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FuncCallExpression extends Expression {
    private final Ident ident;
    private final ImmutableList<Expression> args;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BinaryExpression extends Expression {
    private final Ident ident;
    private final Expression left;
    private final Expression right;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FunctionCallExpression extends Expression {
    private final Ident ident;
    private final ImmutableList<Expression> args;
  }

  private final Node root;

}
