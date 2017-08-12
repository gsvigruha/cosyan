package com.cosyan.db.sql;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.AggrTableMeta;
import com.cosyan.db.model.TableMeta.DerivedTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class SyntaxTree {

  public static enum AggregationExpression {
    YES, NO, EITHER
  }

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

    public DerivedColumn compile(
        TableMeta sourceTable,
        MetaRepo metaRepo) throws ModelException {
      List<AggrColumn> aggrColumns = new LinkedList<>();
      DerivedColumn column = compile(sourceTable, metaRepo, aggrColumns);
      if (!aggrColumns.isEmpty()) {
        throw new ModelException("Aggregators are not allowed here.");
      }
      return column;
    }

    public abstract DerivedColumn compile(
        TableMeta sourceTable,
        MetaRepo metaRepo,
        List<AggrColumn> aggrColumns) throws ModelException;

    public String getName(String def) {
      return def;
    }

    public abstract AggregationExpression isAggregation();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Select extends Node {
    private final ImmutableList<Expression> columns;
    private final Table table;
    private final Optional<Expression> where;
    private final Optional<ImmutableList<Expression>> groupBy;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(metaRepo);
      TableMeta filteredTable = Compiler.filteredTable(metaRepo, sourceTable, where);
      final TableMeta intermediateTable;
      if (groupBy.isPresent()) {
        intermediateTable = Compiler.groupByTable(metaRepo, filteredTable, groupBy);
      } else {
        intermediateTable = filteredTable;
      }
      List<AggrColumn> aggrColumns = new LinkedList<>();
      ImmutableMap<String, ColumnMeta> tableColumns = Compiler.tableColumns(metaRepo, intermediateTable, columns,
          aggrColumns);
      final TableMeta topLevelSourceTable;
      final boolean isAggregation = !aggrColumns.isEmpty();
      if (isAggregation) {
        topLevelSourceTable = new AggrTableMeta(
            intermediateTable,
            ImmutableList.copyOf(aggrColumns),
            ColumnMeta.TRUE_COLUMN);
      } else {
        topLevelSourceTable = intermediateTable;
      }
      return new DerivedTableMeta(topLevelSourceTable, tableColumns);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Table extends Node {
    public abstract ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return metaRepo.table(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return select.compile(metaRepo);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) throws ModelException {
      return expr.compile(sourceTable, metaRepo, aggrColumns);
    }

    @Override
    public AggregationExpression isAggregation() {
      return expr.isAggregation();
    }

    @Override
    public String getName(String def) {
      return ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) throws ModelException {
      if (ident.getString().equals(Tokens.NOT)) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo, aggrColumns);
        assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return !((Boolean) exprColumn.getValue(sourceValues));
          }
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public AggregationExpression isAggregation() {
      return expr.isAggregation();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IdentExpression extends Expression {
    private final Ident ident;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) throws ModelException {
      final String key = ident.getString();
      if (sourceTable.columns().containsKey(key)) {
        // Guava immutable collections keep iteration order;
        final int index = sourceTable.indexOf(ident);
        return new DerivedColumn(sourceTable.columns().get(key).getType()) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return sourceValues[index];
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

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.NO;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression {
    private final String val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.StringType) {

        @Override
        public Object getValue(Object[] sourceValues) {
          return val;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression {
    private final long val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.LongType) {

        @Override
        public Object getValue(Object[] sourceValues) {
          return val;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression {
    private final Double val;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.DoubleType) {

        @Override
        public Object getValue(Object[] sourceValues) {
          return val;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FuncCallExpression extends Expression {
    private final Ident ident;
    private final ImmutableList<Expression> args;

    private boolean isAggr() {
      return BuiltinFunctions.AGGREGATIONS.containsKey(ident.getString());
    }

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns)
        throws ModelException {
      if (isAggr()) {
        if (args.size() != 1) {
          throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
        }
        DerivedColumn argColumn = Iterables.getOnlyElement(args).compile(sourceTable, metaRepo);
        final TypedAggrFunction<?> function = metaRepo.aggrFunction(ident, argColumn.getType());

        AggrColumn aggrColumn = new AggrColumn(
            function.getReturnType(), argColumn, sourceTable.keyColumns().size() + aggrColumns.size()) {
          @Override
          public Object aggregate(Object x, Object y) {
            return function.aggregate(x, y);
          }
        };
        aggrColumns.add(aggrColumn);
        return aggrColumn;
      } else {
        SimpleFunction<?> function = metaRepo.simpleFunction(ident);
        ImmutableList.Builder<DerivedColumn> argColumnsBuilder = ImmutableList.builder();
        for (int i = 0; i < function.getArgTypes().size(); i++) {
          DerivedColumn col = args.get(i).compile(sourceTable, metaRepo);
          assertType(function.getArgTypes().get(i), col.getType());
          argColumnsBuilder.add(col);
        }
        ImmutableList<DerivedColumn> argColumns = argColumnsBuilder.build();

        return new DerivedColumn(function.getReturnType()) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return function.call(
                ImmutableList.copyOf(argColumns.stream().map(col -> col.getValue(sourceValues)).iterator()));
          }
        };
      }
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.NO;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName(String def) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.NO;
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
