package com.cosyan.db.sql;

import java.util.Optional;

import com.cosyan.db.model.BuiltinFunctions.Function;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.DerivedTableMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class SyntaxTree {

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class Node {

  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Ident extends Node {
    private final String string;

    public String[] parts() {
      return string.split("\\.");
    }

    public boolean is(String str) {
      return string.equals(str);
    }

    public boolean is(char c) {
      return string.equals(String.valueOf(c));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Expression extends Node {
    public abstract DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) throws ModelException;

    public String getName(String def) {
      return def;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Select extends Node {
    private final ImmutableList<Expression> columns;
    private final Table table;
    private final Optional<Expression> where;

    public TableMeta compile(MetaRepo metaRepo) throws ModelException {
      TableMeta sourceTable = table.compile(metaRepo);
      ImmutableMap.Builder<String, ColumnMeta> tableColumns = ImmutableMap.builder();
      int i = 0;
      for (Expression expr : columns) {
        if (expr instanceof AsteriskExpression) {
          tableColumns.putAll(sourceTable.columns());
        } else {
          tableColumns.put(expr.getName("_c" + (i++)), expr.compile(sourceTable, metaRepo));
        }
      }
      return new DerivedTableMeta(sourceTable, tableColumns.build());
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Table extends Node {
    public abstract TableMeta compile(MetaRepo metaRepo) throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    public TableMeta compile(MetaRepo metaRepo) throws ModelException {
      return metaRepo.table(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public TableMeta compile(MetaRepo metaRepo) throws ModelException {
      return select.compile(metaRepo);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) throws ModelException {
      if (ident.getString().equals(Tokens.NOT)) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo);
        assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(ImmutableMap<String, Object> sourceValues) {
            return !((Boolean) exprColumn.getValue(sourceValues));
          }
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IdentExpression extends Expression {
    private final Ident ident;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) throws ModelException {
      final String key = ident.getString();
      if (sourceTable.columns().containsKey(key)) {
        return new DerivedColumn(sourceTable.columns().get(key).getType()) {

          @Override
          public Object getValue(ImmutableMap<String, Object> sourceValues) {
            return sourceValues.get(key);
          }
        };
      } else {
        throw new ModelException(
            "Column '" + key + "' does not exist in " + sourceTable.columns().keySet() + ".");
      }
    }

    @Override
    public String getName(String def) {
      return ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression {
    private final String val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) {
      return new DerivedColumn(DataTypes.StringType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return val;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression {
    private final long val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) {
      return new DerivedColumn(DataTypes.LongType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return val;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression {
    private final Double val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) {
      return new DerivedColumn(DataTypes.DoubleType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return val;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FuncCallExpression extends Expression {
    private final Ident ident;
    private final ImmutableList<Expression> args;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) throws ModelException {
      Function<?> function = metaRepo.function(ident);
      ImmutableList.Builder<DerivedColumn> argColumnsBuilder = ImmutableList.builder();
      for (int i = 0; i < function.getArgTypes().size(); i++) {
        DerivedColumn col = args.get(i).compile(sourceTable, metaRepo);
        assertType(function.getArgTypes().get(i), col.getType());
        argColumnsBuilder.add(col);
      }
      ImmutableList<DerivedColumn> argColumns = argColumnsBuilder.build();

      return new DerivedColumn(function.getReturnType()) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return function.call(
              ImmutableList.copyOf(argColumns.stream().map(col -> col.getValue(sourceValues)).iterator()));
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName(String def) {
      throw new UnsupportedOperationException();
    }
  }

  private final Node root;

  public boolean isSelect() {
    return root instanceof Select;
  }

  public static void assertType(DataType<?> expectedType, DataType<?> dataType) throws ModelException {
    if (expectedType != dataType) {
      throw new ModelException(
          "Data type " + dataType + " did not match expected type " + expectedType + ".");
    }
  }

}
