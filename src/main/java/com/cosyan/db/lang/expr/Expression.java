package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.TableContext;
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
  
  public abstract Loc loc();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {

    public static enum Type {
      NOT, ASC, DESC, IS_NULL, IS_NOT_NULL
    }

    private final Type type;
    private final Expression expr;
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
      if (type == Type.NOT) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        SyntaxTree.assertType(DataTypes.BoolType, exprColumn.getType(), expr.loc());
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
            return !((Boolean) exprColumn.value(values, resources, context));
          }

          @Override
          public String print(Object[] values, Resources resources, TableContext context) throws IOException {
            return "not " + exprColumn.print(values, resources, context);
          }
        };
      } else if (type == Type.ASC) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new OrderColumn(exprColumn, true);
      } else if (type == Type.DESC) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new OrderColumn(exprColumn, false);
      } else if (type == Type.IS_NOT_NULL) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
            return exprColumn.value(values, resources, context) != null;
          }

          @Override
          public String print(Object[] values, Resources resources, TableContext context) throws IOException {
            return exprColumn.print(values, resources, context) + " is not null";
          }
        };
      } else if (type == Type.IS_NULL) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable);
        return new DerivedColumnWithDeps(
            DataTypes.BoolType,
            exprColumn.tableDependencies(),
            exprColumn.readResources()) {

          @Override
          public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
            return exprColumn.value(values, resources, context) == null;
          }

          @Override
          public String print(Object[] values, Resources resources, TableContext context) throws IOException {
            return exprColumn.print(values, resources, context) + " is null";
          }
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public String print() {
      return type.name().toLowerCase() + " " + expr.print();
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }
}