package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.Resources;

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
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources) throws IOException {
            return !((Boolean) exprColumn.value(values, resources));
          }

          @Override
          public String print(Object[] values, Resources resources) throws IOException {
            return "not " + exprColumn.print(values, resources);
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
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources) throws IOException {
            return exprColumn.value(values, resources) != DataTypes.NULL;
          }

          @Override
          public String print(Object[] values, Resources resources) throws IOException {
            return exprColumn.print(values, resources) + " is not null";
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources) throws IOException {
            return exprColumn.value(values, resources) == DataTypes.NULL;
          }

          @Override
          public String print(Object[] values, Resources resources) throws IOException {
            return exprColumn.print(values, resources) + " is null";
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