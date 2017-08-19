package com.cosyan.db.sql;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.AggrTableMeta;
import com.cosyan.db.model.TableMeta.AliasedTableMeta;
import com.cosyan.db.model.TableMeta.DerivedTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.JoinTableMeta;
import com.cosyan.db.model.TableMeta.KeyValueTableMeta;
import com.cosyan.db.model.TableMeta.SortedTableMeta;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.base.Joiner;
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

    public boolean isSimple() {
      return parts().length == 1;
    }

    public String head() {
      return parts()[0];
    }

    public Ident tail() {
      return new Ident(Joiner.on(".").join(Arrays.copyOfRange(parts(), 1, parts().length)));
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
    private final Optional<Expression> having;
    private final Optional<ImmutableList<Expression>> orderBy;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(metaRepo);
      ExposedTableMeta filteredTable = Compiler.filteredTable(metaRepo, sourceTable, where);
      DerivedTableMeta fullTable;
      if (Compiler.isAggregation(columns) || groupBy.isPresent()) {
        KeyValueTableMeta intermediateTable = Compiler.keyValueTable(metaRepo, filteredTable, groupBy);
        List<AggrColumn> aggrColumns = new LinkedList<>();
        ImmutableMap<String, ColumnMeta> tableColumns = Compiler.tableColumns(metaRepo, intermediateTable, columns,
            aggrColumns);
        DerivedColumn havingColumn = Compiler.havingExpression(metaRepo, intermediateTable, having,
            aggrColumns);
        fullTable = new DerivedTableMeta(new AggrTableMeta(
            intermediateTable,
            ImmutableList.copyOf(aggrColumns),
            havingColumn), tableColumns);
      } else {
        ImmutableMap<String, ColumnMeta> tableColumns = Compiler.tableColumns(metaRepo, filteredTable, columns);
        fullTable = new DerivedTableMeta(filteredTable, tableColumns);
      }
      if (orderBy.isPresent()) {
        ImmutableList<OrderColumn> orderColumns = Compiler.orderColumns(metaRepo, fullTable, orderBy.get());
        return new SortedTableMeta(fullTable, orderColumns);
      } else {
        return fullTable;
      }
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
  public static class JoinExpr extends Table {
    private final Token joinType;
    private final Table left;
    private final Table right;
    private final Expression onExpr;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta leftTable = left.compile(metaRepo);
      ExposedTableMeta rightTable = right.compile(metaRepo);
      ImmutableList.Builder<ColumnMeta> leftJoinColumns = ImmutableList.builder();
      ImmutableList.Builder<ColumnMeta> rightJoinColumns = ImmutableList.builder();
      ImmutableList<BinaryExpression> exprs = ImmutableList
          .copyOf(decompose(onExpr, new LinkedList<BinaryExpression>()));
      for (BinaryExpression expr : exprs) {
        leftJoinColumns.add(expr.getLeft().compile(leftTable, metaRepo));
        rightJoinColumns.add(expr.getRight().compile(rightTable, metaRepo));
      }
      return new JoinTableMeta(joinType, leftTable, rightTable, leftJoinColumns.build(), rightJoinColumns.build());
    }

    private List<BinaryExpression> decompose(Expression expr, LinkedList<BinaryExpression> collector)
        throws ModelException {
      if (expr instanceof BinaryExpression) {
        BinaryExpression binaryExpr = (BinaryExpression) expr;
        if (binaryExpr.getIdent().getString().equals(Tokens.AND)) {
          decompose(binaryExpr.getLeft(), collector);
          decompose(binaryExpr.getRight(), collector);
        } else if (binaryExpr.getIdent().getString().equals(String.valueOf(Tokens.EQ))) {
          collector.add(binaryExpr);
        } else {
          throw new ModelException(
              "Only 'and' and '=' binary expressions are allowed in the 'on' expression of joins.");
        }
      }
      return collector;
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
  public static class AsTable extends Table {
    private final Ident ident;
    private final Table table;

    @Override
    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return new AliasedTableMeta(ident, table.compile(metaRepo));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Token token;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) throws ModelException {
      if (token.is(Tokens.NOT)) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo, aggrColumns);
        assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return !((Boolean) exprColumn.getValue(sourceValues));
          }
        };
      } else if (token.is(Tokens.ASC)) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo, aggrColumns);
        return new OrderColumn(exprColumn, true);
      } else if (token.is(Tokens.DESC)) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo, aggrColumns);
        return new OrderColumn(exprColumn, false);
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL).getString())) {
        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo, aggrColumns);
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return exprColumn.getValue(sourceValues) != DataTypes.NULL;
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {

        DerivedColumn exprColumn = expr.compile(sourceTable, metaRepo,
            aggrColumns);
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(Object[] sourceValues) {
            return exprColumn.getValue(sourceValues) == DataTypes.NULL;
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
      // Guava immutable collections keep iteration order;
      final int index = sourceTable.indexOf(ident);
      if (index < 0) {
        throw new ModelException(
            "Column '" + ident + "' does not exist.");
      }
      return new DerivedColumn(sourceTable.column(ident).getType()) {

        @Override
        public Object getValue(Object[] sourceValues) {
          return sourceValues[index];
        }
      };
    }

    @Override
    public String getName(String def) {
      if (ident.isSimple()) {
        return ident.getString();
      } else {
        return ident.tail().getString();
      }
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
    private final AggregationExpression aggregation;

    public FuncCallExpression(Ident ident, ImmutableList<Expression> args) throws ParserException {
      this.ident = ident;
      this.args = args;
      if (isAggr()) {
        aggregation = AggregationExpression.YES;
      } else {
        long yes = args.stream().filter(arg -> arg.isAggregation() == AggregationExpression.YES).count();
        long no = args.stream().filter(arg -> arg.isAggregation() == AggregationExpression.NO).count();
        if (no == 0 && yes == 0) {
          aggregation = AggregationExpression.EITHER;
        } else if (no == 0) {
          aggregation = AggregationExpression.YES;
        } else if (yes == 0) {
          aggregation = AggregationExpression.NO;
        } else {
          throw new ParserException("Inconsistent parameter.");
        }
      }
    }

    private boolean isAggr() {
      return BuiltinFunctions.AGGREGATION_NAMES.contains(ident.getString());
    }

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns)
        throws ModelException {
      if (isAggr()) {
        if (args.size() != 1) {
          throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
        }
        if (!(sourceTable instanceof KeyValueTableMeta)) {
          throw new ModelException("Aggregators are not allowed here.");
        }
        KeyValueTableMeta outerTable = (KeyValueTableMeta) sourceTable;
        DerivedColumn argColumn = Iterables.getOnlyElement(args).compile(outerTable.getSourceTable(), metaRepo);
        final TypedAggrFunction<?> function = metaRepo.aggrFunction(ident, argColumn.getType());

        AggrColumn aggrColumn = new AggrColumn(
            function.getReturnType(), argColumn, outerTable.getKeyColumns().size() + aggrColumns.size(), function);
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
            ImmutableList<Object> params = ImmutableList
                .copyOf(argColumns.stream().map(col -> col.getValue(sourceValues)).iterator());
            for (Object param : params) {
              if (param == DataTypes.NULL) {
                return DataTypes.NULL;
              }
            }
            return function.call(params);
          }
        };
      }
    }

    @Override
    public AggregationExpression isAggregation() {
      return aggregation;
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
