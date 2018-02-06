package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.transaction.Resources;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.TableMeta;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class Expression extends Node {

  public ColumnMeta compileColumn(TableMeta sourceTable) throws ModelException {
    CompiledObject obj = compile(sourceTable);
    return (ColumnMeta) obj;
  }

  public abstract CompiledObject compile(TableMeta sourceTable) throws ModelException;

  public String getName(String def) {
    return def;
  }

  public abstract String print();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Token token;
    private final Expression expr;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
      if (token.is(Tokens.NOT)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        SyntaxTree.assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources(),
            exprColumn.tables()) {

          @Override
          public Object getValue(Object[] values, Resources resources) throws IOException {
            return !((Boolean) exprColumn.getValue(values, resources));
          }
        };
      } else if (token.is(Tokens.ASC)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new OrderColumn(exprColumn, true);
      } else if (token.is(Tokens.DESC)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new OrderColumn(exprColumn, false);
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources(),
            exprColumn.tables()) {

          @Override
          public Object getValue(Object[] values, Resources resources) throws IOException {
            return exprColumn.getValue(values, resources) != DataTypes.NULL;
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources(),
            exprColumn.tables()) {

          @Override
          public Object getValue(Object[] values, Resources resources) throws IOException {
            return exprColumn.getValue(values, resources) == DataTypes.NULL;
          }
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public String print() {
      return token.getString() + " " + expr.print();
    }
  }
}