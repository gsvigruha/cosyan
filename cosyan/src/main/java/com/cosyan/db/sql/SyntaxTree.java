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
  public static class ColumnExpr extends Expression {

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnRef extends ColumnExpr {
    private final Ident ident;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AllColumns extends ColumnExpr {
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnAggr extends ColumnExpr {
    private final String aggr;
    private final Ident col;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Select extends Node {
    private final ImmutableList<ColumnExpr> columns;
    private final Table table;
    private final Optional<Where> where;
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
  public static class Where extends Node {
    private final Expression expr;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Token symbol;
    private final Expression expr;
  }
  
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IdentExpression extends Expression {
    private final Ident ident;
  }
  
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BinaryExpression extends Expression {
    private final Token symbol;
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
